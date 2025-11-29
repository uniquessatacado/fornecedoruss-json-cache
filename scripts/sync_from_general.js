/* scripts/sync_from_general_fixed.js
   Versão corrigida — combate agressivo a "0000-00-00..." + tratamento de erros
*/

const fs = require('fs');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing Supabase credentials (SUPABASE_URL / SUPABASE_KEY)");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });

const COLUMNS_CLIENTES = [
  'cliente_codigo','codigo','nome','email','data_cadastro',
  'whatsapp','cidade','estado','loja_drop','representante',
  'total_pedidos','valor_total_comprado','criado_em'
];

const COLUMNS_PEDIDOS = [
  'id','codigo_pedido','cliente_codigo','situacao_pedido',
  'data_hora_pedido','data_hora_confirmacao',
  'valor_total_produtos','valor_frete','frete','valor_total_pedido',
  'desconto','cidade','estado','percentual_comissao',
  'origem_pedido','tipo_compra','texto_tipo_compra',
  'pedidos_loja_drop','criado_em'
];

const COLUMNS_PRODUTOS = [
  'id','cliente_codigo','produto_codigo','titulo','categoria_principal',
  'categoria','marca','quantidade','criado_em','id_pedido','valor_unitario',
  'subcategoria','tamanho','cor','sku','data_pedido'
];

/* ---------------- helpers ---------------- */

function pickFields(obj, allowed) {
  const res = {};
  for (const k of Object.keys(obj || {})) {
    if (allowed.includes(k)) res[k] = obj[k];
  }
  return res;
}

function findArrayByHeuristics(json, candidateNames = []) {
  for (const name of candidateNames) {
    if (Array.isArray(json[name])) return json[name];
  }
  for (const k of Object.keys(json)) {
    if (Array.isArray(json[k])) return json[k];
  }
  if (Array.isArray(json)) return json;
  return [];
}

function parseDateString(val) {
  if (!val || typeof val !== "string") return null;
  const s = val.trim();
  const m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/);
  if (!m) return null;
  const day = m[1], month = m[2], year = m[3];
  let time = m[4] || "00:00:00";
  if (/^\d{2}:\d{2}$/.test(time)) time = time + ":00";
  return `${year}-${month}-${day}T${time}Z`;
}

function isLikelyZeroDate(s) {
  if (!s || typeof s !== 'string') return false;
  // cobre vários formatos; se houver '0000' em ano ou dia com zeros, considera inválido
  if (/0000-00-00/.test(s)) return true;
  if (/^0{4}-0{2}-0{2}/.test(s)) return true;
  if (/^0000[\/\-]/.test(s)) return true;
  if (/^0{8}$/.test(s.replace(/[^0-9]/g, ''))) return true;
  return false;
}

function sanitizeDateValue(v) {
  if (v === null || v === undefined) return null;
  if (v instanceof Date) {
    if (isNaN(v.getTime())) return null;
    return v.toISOString(); // manter ISO se Date real
  }
  if (typeof v !== "string") return null;
  const s = v.trim();
  if (s === '') return null;
  // se claramente zero-date -> null (evita inserir 0000-00-00T00:00:00Z)
  if (isLikelyZeroDate(s) || s.startsWith('0000')) return null;

  // já está no formato YYYY-MM-DDTHH:MM:SSZ (aceitar apenas se plausível)
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(s)) {
    // rejeitar datas impossíveis (ano 0000)
    if (/^0000-/.test(s)) return null;
    const ts = Date.parse(s);
    return isNaN(ts) ? null : s;
  }

  // se for "YYYY-MM-DD HH:MM:SS" -> transformar para ISO
  if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/.test(s)) {
    const s2 = s.replace(' ', 'T') + 'Z';
    if (/^0000-/.test(s2)) return null;
    const ts = Date.parse(s2);
    return isNaN(ts) ? null : s2;
  }

  // tentar dd/mm/yyyy[ hh:mm:ss]
  const p = parseDateString(s);
  if (p) return p;

  // outros formatos: rejeitar para evitar lixo
  return null;
}

function normalizeCodigo(val) {
  if (val === null || val === undefined) return null;
  let s = String(val).trim();
  if (s === '') return null;
  s = s.replace(/[^\w\-\._]/g, '');
  return s;
}

function toNumberOrNull(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  const s = String(v).replace(/[^\d\-.,]/g, '').trim();
  if (s === '') return null;
  const s2 = s.replace(/\./g, '').replace(/,/g, '.');
  const n = parseFloat(s2);
  return Number.isNaN(n) ? null : n;
}

function dedupeByKey(rows, keys = ['codigo']) {
  const map = new Map();
  for (const r of rows) {
    const key = keys.map(k0 => (r[k0] === undefined || r[k0] === null) ? '' : String(r[k0])).join('|');
    if (!map.has(key)) map.set(key, r);
  }
  return Array.from(map.values());
}

/* Insert robusto: menor batch, delay e fallback item-by-item em caso de timeout */
async function insertInBatchesRobust(table, rows, batch = 100, delayMs = 400) {
  // antes, garantir remover qualquer string perigosa "0000-00-00T00:00:00Z" em rows
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    for (const k of Object.keys(row)) {
      const v = row[k];
      if (typeof v === 'string' && isLikelyZeroDate(v)) row[k] = null;
      // também remover strings exatas problemáticas
      if (v === '0000-00-00T00:00:00Z' || v === '0000-00-00 00:00:00') row[k] = null;
    }
  }

  for (let i = 0; i < rows.length; i += batch) {
    const chunk = rows.slice(i, i + batch);
    if (chunk.length === 0) continue;
    try {
      const { error } = await supabase.from(table).insert(chunk, { returning: false });
      if (error) {
        console.error(`Error inserting chunk into ${table} (offset ${i})`, error);
        console.log(`Attempting per-row retry for ${table} (offset ${i})...`);
        for (let r = 0; r < chunk.length; r++) {
          try {
            const row = chunk[r];
            // scrub again
            for (const k of Object.keys(row)) {
              const v = row[k];
              if (typeof v === 'string' && isLikelyZeroDate(v)) row[k] = null;
            }
            const { error: e2 } = await supabase.from(table).insert([row], { returning: false });
            if (e2) {
              console.error(`Row insert error ${table} offset ${i} row ${r}:`, e2);
            }
          } catch (e3) {
            console.error(`Row insert thrown ${table} offset ${i} row ${r}:`, e3);
          }
        }
      } else {
        console.log(`Inserted ${chunk.length} into ${table} (offset ${i})`);
      }
    } catch (e) {
      console.error(`Exception inserting chunk into ${table} (offset ${i}):`, e);
      for (let r = 0; r < chunk.length; r++) {
        try {
          const row = chunk[r];
          for (const k of Object.keys(row)) {
            const v = row[k];
            if (typeof v === 'string' && isLikelyZeroDate(v)) row[k] = null;
          }
          const { error: e2 } = await supabase.from(table).insert([row], { returning: false });
          if (e2) console.error(`Row insert error ${table} offset ${i} row ${r}:`, e2);
        } catch (e3) {
          console.error(`Row insert thrown ${table} offset ${i} row ${r}:`, e3);
        }
      }
    }
    await new Promise(res => setTimeout(res, delayMs));
  }
}

/* delete padrão */
async function deleteAll(table) {
  try {
    const { error } = await supabase.from(table).delete().gt('id', 0);
    if (error) {
      console.log(`delete fallback for ${table}`, error.message || error);
      await supabase.from(table).delete().not('id', 'is', null);
    } else {
      console.log(`Deleted contents of ${table}`);
    }
  } catch (e) {
    console.error("delete error", e);
    throw e;
  }
}

/* ------------------ main ------------------ */
async function main() {
  try {
    const source = process.argv[2];
    if (!source || !fs.existsSync(source)) {
      console.error("Arquivo não encontrado:", source);
      process.exit(1);
    }

    const raw = fs.readFileSync(source, 'utf8');
    let json;
    try { json = JSON.parse(raw); } catch (e) { console.error("Erro parseando JSON:", e.message); process.exit(1); }

    // Detect arrays like antes (mantive sua heurística)
    const clientesArr = findArrayByHeuristics(json, ['clientes','lista_clientes','lista_clientes_geral','clientes_lista','clientes_data','users']);
    // Se o arquivo que você usa tem clientes embutidos (formato que você mostrou), a heurística pode retornar o grande array corretamente
    // Para segurança, se encontrou clientes por heurística = ok, senão assume json se for array
    const clientesSource = Array.isArray(clientesArr) && clientesArr.length ? clientesArr : (Array.isArray(json) ? json : []);
    console.log(`→ ORIGINAIS:\nClientes: ${clientesSource.length}`);

    // clientes
    let clientesRows = clientesSource.map(it => pickFields(it, COLUMNS_CLIENTES));
    clientesRows = clientesRows.map(row => {
      const copy = { ...row };
      copy.codigo = copy.codigo ?? copy.cliente_codigo ?? copy.id ?? copy.codigo_cliente ?? null;
      copy.codigo = normalizeCodigo(copy.codigo);
      if (!copy.cliente_codigo && copy.codigo) copy.cliente_codigo = copy.codigo;
      copy.cliente_codigo = normalizeCodigo(copy.cliente_codigo);
      copy.total_pedidos = (copy.total_pedidos !== undefined) ? parseInt(String(copy.total_pedidos).replace(/\D/g,''),10) || null : null;
      copy.valor_total_comprado = toNumberOrNull(copy.valor_total_comprado);
      if (copy.data_cadastro) copy.data_cadastro = sanitizeDateValue(copy.data_cadastro);
      // garantir criado_em sem horário problemático (usar apenas ISO full or date)
      copy.criado_em = (new Date().toISOString()).replace(/\.\d+Z$/, 'Z');
      // scrub zeros
      for (const k of Object.keys(copy)) {
        if (typeof copy[k] === 'string' && isLikelyZeroDate(copy[k])) copy[k] = null;
      }
      return copy;
    });

    // dedupe clientes
    clientesRows = clientesRows.map(c => {
      if (!c.codigo && c.cliente_codigo) c.codigo = c.cliente_codigo;
      return c;
    });
    clientesRows = dedupeByKey(clientesRows, ['codigo']);
    console.log(`→ DEDUPE FINAL: ${clientesRows.length} clientes únicos (orig ${clientesSource.length})`);

    console.log("→ LIMPAR TABELAS...");
    await deleteAll('import_clientes');

    // inserir clientes em batches via upsert (para evitar duplicate errors)
    if (clientesRows.length) {
      const batch = 300;
      for (let i = 0; i < clientesRows.length; i += batch) {
        const chunk = clientesRows.slice(i, i + batch).map(r => {
          const copy = { ...r };
          for (const k of Object.keys(copy)) {
            if (typeof copy[k] === 'string' && isLikelyZeroDate(copy[k])) copy[k] = null;
            if (/data|criado|hora|date|timestamp/i.test(k) && copy[k]) copy[k] = sanitizeDateValue(copy[k]);
          }
          if ((!copy.cliente_codigo || copy.cliente_codigo === '') && copy.codigo) copy.cliente_codigo = copy.codigo;
          return copy;
        });

        try {
          const { error } = await supabase.from('import_clientes').upsert(chunk, { onConflict: 'codigo' });
          if (error) {
            console.error(`Erro em upsert import_clientes (offset ${i})`, error);
            // tentar linha a linha mas não interromper
            for (let r = 0; r < chunk.length; r++) {
              try {
                const { error: e2 } = await supabase.from('import_clientes').upsert([chunk[r]], { onConflict: 'codigo' });
                if (e2) console.error('upsert row error', e2);
              } catch (er) { console.error('upsert row exception', er); }
            }
          } else {
            console.log(`Upserted ${chunk.length} into import_clientes (offset ${i})`);
          }
        } catch (e) {
          console.error(`Exception upserting clients offset ${i}:`, e);
          // tentar por linha para isolar
          for (let r = 0; r < chunk.length; r++) {
            try {
              const row = chunk[r];
              for (const k of Object.keys(row)) {
                if (typeof row[k] === 'string' && isLikelyZeroDate(row[k])) row[k] = null;
              }
              const { error: e2 } = await supabase.from('import_clientes').upsert([row], { onConflict: 'codigo' });
              if (e2) console.error('row upsert fallback error', e2);
            } catch (er) { console.error('row upsert fallback exception', er); }
          }
        }
      }
    }

    // Agora precisamos extrair pedidos e produtos do mesmo JSON (se estiverem embutidos)
    // Usar heurística simples: para cada cliente original, se existir cliente.pedidos -> extrair
    const pedidosRows = [];
    const produtosRows = [];

    for (let idx = 0; idx < clientesSource.length; idx++) {
      const client = clientesSource[idx] || {};
      const clienteCodigo = normalizeCodigo(client.codigo ?? client.cliente_codigo ?? client.id ?? '') || null;

      // pedidos embutidos
      if (Array.isArray(client.pedidos)) {
        for (const rawItem of client.pedidos) {
          const p = pickFields(rawItem, COLUMNS_PEDIDOS);

          p.codigo_pedido = p.codigo_pedido ?? rawItem.codigo_pedido ?? rawItem.codigo ?? rawItem.numero_pedido ?? rawItem.order_id ?? rawItem.id ?? null;
          p.codigo_pedido = normalizeCodigo(p.codigo_pedido);

          p.cliente_codigo = normalizeCodigo(clienteCodigo ?? rawItem.cliente_codigo ?? rawItem.cliente ?? rawItem.codigo_cliente ?? rawItem.customer_id ?? rawItem.id_cliente ?? null) || '0';
          p.situacao_pedido = p.situacao_pedido ?? rawItem.status ?? rawItem.situacao ?? rawItem.status_pedido ?? null;

          // datas
          p.data_hora_pedido = sanitizeDateValue(rawItem.data_hora_pedido ?? rawItem.data_pedido ?? rawItem.data_hora ?? rawItem.created_at ?? rawItem.criado_em ?? null);
          p.data_hora_confirmacao = sanitizeDateValue(rawItem.data_hora_confirmacao ?? rawItem.confirmado_em ?? rawItem.paid_at ?? null);

          // valores
          p.valor_total_produtos = toNumberOrNull(rawItem.valor_total_produtos ?? rawItem.valor_produtos ?? rawItem.items_total ?? rawItem.total_items ?? rawItem.total ?? null);
          p.valor_frete = toNumberOrNull(rawItem.valor_frete ?? rawItem.shipping_value ?? rawItem.frete ?? null);
          p.valor_total_pedido = toNumberOrNull(rawItem.valor_total_pedido ?? rawItem.total ?? null);
          p.desconto = toNumberOrNull(rawItem.desconto ?? rawItem.valor_desconto ?? rawItem.discount ?? null);
          p.percentual_comissao = toNumberOrNull(rawItem.percent ?? rawItem.comissao ?? null);

          p.cidade = rawItem.cidade ?? client.cidade ?? null;
          p.estado = rawItem.estado ?? client.estado ?? null;
          p.origem_pedido = rawItem.origem_pedido ?? null;
          p.tipo_compra = rawItem.tipo_compra ?? null;
          p.texto_tipo_compra = rawItem.texto_tipo_compra ?? null;
          p.pedidos_loja_drop = rawItem.pedidos_loja_drop ?? null;
          p.criado_em = new Date().toISOString();

          // scrub zero-dates just in case
          for (const k of Object.keys(p)) {
            if (typeof p[k] === 'string' && isLikelyZeroDate(p[k])) p[k] = null;
          }

          // garantir codigo_pedido
          if (!p.codigo_pedido) p.codigo_pedido = `P_${clienteCodigo || '0'}_${Math.floor(Math.random()*1e9)}`;

          pedidosRows.push(p);
        }
      }

      // produtos_embutidos (obj produtos_comprados)
      const produtosComprados = client.produtos_comprados;
      if (produtosComprados && typeof produtosComprados === 'object') {
        for (const key of Object.keys(produtosComprados)) {
          const prRaw = produtosComprados[key] || {};
          const produto_codigo = normalizeCodigo(prRaw.codigo ?? key) || null;
          const produtoRow = {
            cliente_codigo: clienteCodigo || '0',
            produto_codigo,
            titulo: prRaw.titulo ?? prRaw.title ?? null,
            categoria_principal: prRaw.categoria_principal ?? null,
            categoria: prRaw.categoria ?? null,
            marca: prRaw.marca ?? null,
            quantidade: prRaw.quantidade != null ? (parseInt(String(prRaw.quantidade).replace(/\D/g,''),10) || toNumberOrNull(prRaw.quantidade) || null) : null,
            criado_em: new Date().toISOString()
          };
          // scrub
          for (const k of Object.keys(produtoRow)) {
            if (typeof produtoRow[k] === 'string' && isLikelyZeroDate(produtoRow[k])) produtoRow[k] = null;
          }
          produtosRows.push(produtoRow);
        }
      }
    }

    console.log(`→ EXTRAÍDO: pedidos ${pedidosRows.length}, produtos ${produtosRows.length}`);

    // placeholders: garantir '0' e quaisquer cliente_codigo usados por pedidos/produtos que não existam
    const existingSet = new Set(clientesRows.map(c => String(c.codigo ?? c.cliente_codigo ?? '').trim()));
    const usedCodes = new Set();
    for (const p of pedidosRows) usedCodes.add(String(p.cliente_codigo ?? '').trim());
    for (const pr of produtosRows) usedCodes.add(String(pr.cliente_codigo ?? '').trim());
    const missing = Array.from(usedCodes).filter(c => c !== '' && !existingSet.has(c));
    if (usedCodes.has('') || usedCodes.has('0')) {
      if (!existingSet.has('0')) missing.push('0');
    }
    const uniqueMissing = Array.from(new Set(missing));

    if (uniqueMissing.length) {
      console.log(`→ Criando ${uniqueMissing.length} placeholders em import_clientes para satisfazer FK.`);
      const placeholders = uniqueMissing.map(code => ({
        codigo: String(code),
        cliente_codigo: String(code),
        nome: 'AUTO-CREATED',
        criado_em: new Date().toISOString()
      }));
      try {
        for (let i = 0; i < placeholders.length; i += 300) {
          const chunk = placeholders.slice(i, i + 300);
          const { error } = await supabase.from('import_clientes').upsert(chunk, { onConflict: 'codigo' });
          if (error) console.error('Erro criando placeholders', error);
          else console.log(`Placeholders upserted ${chunk.length} (offset ${i})`);
        }
      } catch (e) {
        console.error('Erro criando placeholders', e);
      }
    } else {
      console.log('→ Nenhum placeholder necessário.');
    }

    // garantir cliente_codigo nos pedidos/produtos (fallback '0') e normalizar datas
    for (let idx = 0; idx < pedidosRows.length; idx++) {
      const copy = pedidosRows[idx];
      if (!copy.cliente_codigo || String(copy.cliente_codigo).trim() === '') copy.cliente_codigo = '0';
      if (!copy.codigo_pedido || copy.codigo_pedido === '') {
        copy.codigo_pedido = normalizeCodigo(copy.id ?? copy.codigo_pedido ?? copy.codigo ?? ('P' + idx)) || (`P${idx}`);
      }
      if (copy.data_hora_pedido) copy.data_hora_pedido = sanitizeDateValue(copy.data_hora_pedido);
      if (copy.data_hora_confirmacao) copy.data_hora_confirmacao = sanitizeDateValue(copy.data_hora_confirmacao);
      // scrub final
      for (const k of Object.keys(copy)) if (typeof copy[k] === 'string' && isLikelyZeroDate(copy[k])) copy[k] = null;
    }

    for (let prIdx = 0; prIdx < produtosRows.length; prIdx++) {
      const copy = produtosRows[prIdx];
      if (!copy.cliente_codigo || String(copy.cliente_codigo).trim() === '') copy.cliente_codigo = '0';
      // quantidade normalizada já
      for (const k of Object.keys(copy)) if (typeof copy[k] === 'string' && isLikelyZeroDate(copy[k])) copy[k] = null;
    }

    // inserir pedidos (robusto)
    try {
      await deleteAll('import_pedidos');
      if (pedidosRows.length) {
        await insertInBatchesRobust('import_pedidos', pedidosRows, 100, 400);
      }
    } catch (e) {
      console.error("FATAL ao inserir import_pedidos:", e);
      throw e;
    }

    // inserir produtos (robusto)
    try {
      await deleteAll('import_clientes_produtos');
      if (produtosRows.length) {
        await insertInBatchesRobust('import_clientes_produtos', produtosRows, 200, 300);
      }
    } catch (e) {
      console.error("FATAL ao inserir import_clientes_produtos:", e);
      throw e;
    }

    console.log("Sync finished successfully.");
  } catch (err) {
    console.error("Fatal error in main:", err);
    process.exit(1);
  }
}

process.on('unhandledRejection', (reason, p) => {
  console.error('Unhandled Rejection at:', p, 'reason:', reason);
  // Não deixar o node morrer sem log -- encerra com código de erro
  process.exit(1);
});

main().catch(e => {
  console.error("Fatal error", e);
  process.exit(1);
});
