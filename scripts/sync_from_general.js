/* scripts/sync_from_general.js
   Versão reforçada (2025-11-29) — batch menor, retries e mapeamento mais robusto
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
  return /0000-00-00/.test(s) || /^0{4}-0{2}-0{2}/.test(s);
}

function sanitizeDateValue(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'string') {
    const s = v.trim();
    if (s === '' || isLikelyZeroDate(s)) return null;
    if (/^\d{4}-\d{2}-\d{2}T/.test(s)) {
      const ts = Date.parse(s);
      return isNaN(ts) ? null : s;
    }
    const p = parseDateString(s);
    if (p) return p;
    return null;
  }
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v.toISOString();
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
  for (let i = 0; i < rows.length; i += batch) {
    const chunk = rows.slice(i, i + batch);
    if (chunk.length === 0) continue;
    try {
      const { error } = await supabase.from(table).insert(chunk, { returning: false });
      if (error) {
        // se for timeout (57014) ou outro, tentamos inserir um a um para isolar
        console.error(`Error inserting chunk into ${table} (offset ${i})`, error);
        console.log(`Attempting per-row retry for ${table} (offset ${i})...`);
        for (let r = 0; r < chunk.length; r++) {
          try {
            const { error: e2 } = await supabase.from(table).insert([chunk[r]], { returning: false });
            if (e2) {
              console.error(`Row insert error ${table} offset ${i} row ${r}:`, e2);
              // continue mesmo assim (não interrompe todo o processo)
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
      // fallback item-by-item
      for (let r = 0; r < chunk.length; r++) {
        try {
          const { error: e2 } = await supabase.from(table).insert([chunk[r]], { returning: false });
          if (e2) console.error(`Row insert error ${table} offset ${i} row ${r}:`, e2);
        } catch (e3) {
          console.error(`Row insert thrown ${table} offset ${i} row ${r}:`, e3);
        }
      }
    }
    // pequeno delay para aliviar o banco entre batches
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
  const source = process.argv[2];
  if (!source || !fs.existsSync(source)) {
    console.error("Arquivo não encontrado:", source);
    process.exit(1);
  }

  const raw = fs.readFileSync(source, 'utf8');
  let json;
  try { json = JSON.parse(raw); } catch (e) { console.error("Erro parseando JSON:", e.message); process.exit(1); }

  const clientesArr = findArrayByHeuristics(json, ['clientes','lista_clientes','lista_clientes_geral','clientes_lista','clientes_data','users']) || [];
  const pedidosArr  = findArrayByHeuristics(json, ['pedidos','lista_pedidos','orders','lista_orders','pedidos_lista']) || [];
  const produtosArr = findArrayByHeuristics(json, ['produtos','itens','items','lista_produtos','order_items']) || [];

  console.log(`→ ORIGINAIS:\nClientes: ${clientesArr.length}\nPedidos: ${pedidosArr.length}\nProdutos: ${produtosArr.length}`);

  // clientes
  let clientesRows = clientesArr.map(it => pickFields(it, COLUMNS_CLIENTES));
  clientesRows = clientesRows.map(row => {
    const copy = { ...row };
    copy.codigo = copy.codigo ?? copy.cliente_codigo ?? copy.id ?? copy.codigo_cliente ?? null;
    copy.codigo = normalizeCodigo(copy.codigo);
    if (!copy.cliente_codigo && copy.codigo) copy.cliente_codigo = copy.codigo;
    copy.cliente_codigo = normalizeCodigo(copy.cliente_codigo);
    copy.total_pedidos = (copy.total_pedidos !== undefined) ? parseInt(String(copy.total_pedidos).replace(/\D/g,''),10) || null : null;
    copy.valor_total_comprado = toNumberOrNull(copy.valor_total_comprado);
    if (copy.data_cadastro) copy.data_cadastro = sanitizeDateValue(copy.data_cadastro);
    return copy;
  });

  // pedidos: MAPEAMENTO ROBUSTO (várias chaves alternativas)
  let pedidosRows = pedidosArr.map(rawItem => {
    const p = pickFields(rawItem, COLUMNS_PEDIDOS);

    // fallback smart mapping:
    p.codigo_pedido = p.codigo_pedido ?? rawItem.codigo_pedido ?? rawItem.codigo ?? rawItem.numero_pedido ?? rawItem.order_id ?? rawItem.id ?? null;
    p.codigo_pedido = normalizeCodigo(p.codigo_pedido);

    p.cliente_codigo = p.cliente_codigo ?? rawItem.cliente_codigo ?? rawItem.cliente ?? rawItem.codigo_cliente ?? rawItem.customer_id ?? rawItem.id_cliente ?? null;
    p.cliente_codigo = normalizeCodigo(p.cliente_codigo);

    p.situacao_pedido = p.situacao_pedido ?? rawItem.status ?? rawItem.situacao ?? rawItem.status_pedido ?? null;

    // datas: tentar várias chaves
    const dateCandidates = [
      rawItem.data_hora_pedido, rawItem.data_pedido, rawItem.data_hora,
      rawItem.created_at, rawItem.criado_em, rawItem.data_pedido_br
    ];
    for (const d of dateCandidates) {
      if (d) { p.data_hora_pedido = sanitizeDateValue(d); break; }
    }
    // confirmação
    const confCandidates = [rawItem.data_hora_confirmacao, rawItem.confirmado_em, rawItem.paid_at];
    for (const d of confCandidates) { if (d) { p.data_hora_confirmacao = sanitizeDateValue(d); break; } }

    // valores
    p.valor_total_produtos = p.valor_total_produtos ?? rawItem.valor_produtos ?? rawItem.valor_total_produtos ?? rawItem.items_total ?? toNumberOrNull(rawItem.total_items) ?? null;
    p.valor_total_produtos = toNumberOrNull(p.valor_total_produtos);
    p.valor_frete = p.valor_frete ?? rawItem.valor_frete ?? rawItem.shipping_value ?? toNumberOrNull(rawItem.frete) ?? null;
    p.valor_frete = toNumberOrNull(p.valor_frete);
    p.valor_total_pedido = p.valor_total_pedido ?? rawItem.valor_total_pedido ?? rawItem.total ?? toNumberOrNull(rawItem.valor_total) ?? null;
    p.valor_total_pedido = toNumberOrNull(p.valor_total_pedido);
    p.desconto = p.desconto ?? rawItem.valor_desconto ?? rawItem.discount ?? null;
    p.desconto = toNumberOrNull(p.desconto);
    p.percentual_comissao = p.percentual_comissao ?? rawItem.percent ?? rawItem.comissao ?? null;
    p.percentual_comissao = toNumberOrNull(p.percentual_comissao);

    // garantir cliente_codigo
    if (!p.cliente_codigo || p.cliente_codigo === '') p.cliente_codigo = '0';

    return p;
  });

  // produtos
  let produtosRows = produtosArr.map(it => pickFields(it, COLUMNS_PRODUTOS));
  produtosRows = produtosRows.map(row => {
    const copy = { ...row };
    copy.cliente_codigo = copy.cliente_codigo ?? row.cliente ?? row.codigo_cliente ?? null;
    copy.cliente_codigo = normalizeCodigo(copy.cliente_codigo) ?? '0';
    copy.produto_codigo = copy.produto_codigo ?? row.codigo ?? row.sku ?? null;
    copy.quantidade = copy.quantidade !== undefined ? parseInt(String(copy.quantidade).replace(/\D/g,''),10) || null : null;
    copy.valor_unitario = toNumberOrNull(copy.valor_unitario);
    if (copy.data_pedido) copy.data_pedido = sanitizeDateValue(copy.data_pedido);
    return copy;
  });

  // dedupe clientes por codigo
  clientesRows = clientesRows.map(c => {
    if (!c.codigo && c.cliente_codigo) c.codigo = c.cliente_codigo;
    return c;
  });

  clientesRows = dedupeByKey(clientesRows, ['codigo']);
  console.log(`→ DEDUPE FINAL: ${clientesRows.length} clientes únicos (orig ${clientesArr.length})`);

  console.log("→ LIMPAR TABELAS...");

  // inserir clientes em batches via upsert (para evitar duplicate errors)
  try {
    await deleteAll('import_clientes');
    if (clientesRows.length) {
      // upsert por chunks
      const batch = 300;
      for (let i = 0; i < clientesRows.length; i += batch) {
        const chunk = clientesRows.slice(i, i + batch).map(r => {
          const copy = { ...r };
          // limpar zero-dates
          for (const k of Object.keys(copy)) {
            if (typeof copy[k] === 'string' && isLikelyZeroDate(copy[k])) copy[k] = null;
            if (/data|criado|hora|date|timestamp/i.test(k) && copy[k]) copy[k] = sanitizeDateValue(copy[k]);
          }
          if ((!copy.cliente_codigo || copy.cliente_codigo === '') && copy.codigo) copy.cliente_codigo = copy.codigo;
          return copy;
        });

        const { error } = await supabase.from('import_clientes').upsert(chunk, { onConflict: 'codigo' });
        if (error) {
          console.error(`Erro em upsert import_clientes (offset ${i})`, error);
          // fallback per-row (para não travar tudo)
          for (let r = 0; r < chunk.length; r++) {
            try {
              const { error: e2 } = await supabase.from('import_clientes').upsert([chunk[r]], { onConflict: 'codigo' });
              if (e2) console.error('upsert row error', e2);
            } catch (er) { console.error('upsert row exception', er); }
          }
        } else {
          console.log(`Upserted ${chunk.length} into import_clientes (offset ${i})`);
        }
      }
    }
  } catch (e) {
    console.error("FATAL ao inserir import_clientes:", e);
    throw e;
  }

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

  // garantir cliente_codigo nos pedidos/produtos (fallback '0')
  pedidosRows = pedidosRows.map((p, idx) => {
    const copy = { ...p };
    if (!copy.cliente_codigo || String(copy.cliente_codigo).trim() === '') copy.cliente_codigo = '0';
    if (!copy.codigo_pedido || copy.codigo_pedido === '') {
      // tentar criar um codigo_pedido a partir do id
      copy.codigo_pedido = normalizeCodigo(copy.id ?? copy.codigo_pedido ?? copy.codigo ?? ('P' + idx));
    }
    // normalizar datas
    if (copy.data_hora_pedido) copy.data_hora_pedido = sanitizeDateValue(copy.data_hora_pedido);
    if (copy.data_hora_confirmacao) copy.data_hora_confirmacao = sanitizeDateValue(copy.data_hora_confirmacao);
    return copy;
  });

  produtosRows = produtosRows.map(pr => {
    const copy = { ...pr };
    if (!copy.cliente_codigo || String(copy.cliente_codigo).trim() === '') copy.cliente_codigo = '0';
    return copy;
  });

  // Inserir pedidos (robusto)
  try {
    await deleteAll('import_pedidos');
    if (pedidosRows.length) {
      // usamos insertInBatchesRobust (batch menor, retries, delays)
      await insertInBatchesRobust('import_pedidos', pedidosRows, 100, 400);
    }
  } catch (e) {
    console.error("FATAL ao inserir import_pedidos:", e);
    throw e;
  }

  // Inserir produtos (robusto)
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
}

main().catch(e => {
  console.error("Fatal error", e);
  process.exit(1);
});
