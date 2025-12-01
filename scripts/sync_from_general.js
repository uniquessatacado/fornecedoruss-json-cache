/* scripts/sync_from_general.js
   Versão simplificada: só compara produto_codigo e quantidade.
   - QUANTITY_MODE: 'delta' (default) ou 'absolute'
   - delta: incoming.quantidade é variação (soma ao existente)
   - absolute: incoming.quantidade substitui existente
   - Mantém upsert de clientes e pedidos como antes.
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

/* ====== CONFIG ====== */
// Mude aqui se quiser outro comportamento: 'delta' ou 'absolute'
const QUANTITY_MODE = 'delta'; // 'delta' = soma a quantidade existente; 'absolute' = substitui pelo valor do JSON

const DO_DELETE_ORPHANS = {
  import_clientes: false,
  import_pedidos: false,
  import_clientes_produtos: false
};
/* ===================== */

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
  'categoria','marca','quantidade','criado_em','id_pedido',
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
    return v.toISOString();
  }
  if (typeof v !== "string") return null;
  const s = v.trim();
  if (s === '') return null;
  if (isLikelyZeroDate(s) || s.startsWith('0000')) return null;
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(s)) {
    if (/^0000-/.test(s)) return null;
    const ts = Date.parse(s);
    return isNaN(ts) ? null : s;
  }
  if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/.test(s)) {
    const s2 = s.replace(' ', 'T') + 'Z';
    if (/^0000-/.test(s2)) return null;
    const ts = Date.parse(s2);
    return isNaN(ts) ? null : s2;
  }
  const p = parseDateString(s);
  if (p) return p;
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

/* ---------------- DB helpers (clientes/pedidos) ---------------- */

async function upsertClientesInBatches(rows, batch = 300) {
  for (let i = 0; i < rows.length; i += batch) {
    const chunk = rows.slice(i, i + batch).map(r => {
      const copy = { ...r };
      for (const k of Object.keys(copy)) {
        if (typeof copy[k] === 'string' && isLikelyZeroDate(copy[k])) copy[k] = null;
        if (/data|criado|hora|date|timestamp/i.test(k) && copy[k]) copy[k] = sanitizeDateValue(copy[k]);
      }
      if ((!copy.cliente_codigo || copy.cliente_codigo === '') && copy.codigo) copy.cliente_codigo = copy.codigo;
      return copy;
    });
    const { error } = await supabase.from('import_clientes').upsert(chunk, { onConflict: 'codigo' });
    if (error) console.error('Erro upserting import_clientes chunk:', error);
    else console.log(`Upsert clientes chunk ${i}/${rows.length}`);
  }
}

async function upsertPedidosInBatches(rows, batch = 200) {
  for (let i = 0; i < rows.length; i += batch) {
    const chunk = rows.slice(i, i + batch).map(r => {
      const copy = { ...r };
      for (const k of Object.keys(copy)) {
        if (typeof copy[k] === 'string' && isLikelyZeroDate(copy[k])) copy[k] = null;
        if (/data|criado|hora|date|timestamp/i.test(k) && copy[k]) copy[k] = sanitizeDateValue(copy[k]);
      }
      return copy;
    });
    const { error } = await supabase.from('import_pedidos').upsert(chunk, { onConflict: 'codigo_pedido' });
    if (error) {
      console.error('Erro upserting import_pedidos chunk:', error);
      for (let r = 0; r < chunk.length; r++) {
        const { error: e2 } = await supabase.from('import_pedidos').upsert([chunk[r]], { onConflict: 'codigo_pedido' });
        if (e2) console.error('Row upsert error import_pedidos:', e2);
      }
    } else console.log(`Upsert pedidos chunk ${i}/${rows.length}`);
  }
}

/* ---------------- Core: sync products (only produto_codigo + quantidade) ----------------
   Behavior:
   - For each chunk, fetch existing rows by produto_codigo.
   - If not exists -> insert incoming row.
   - If exists -> update quantidade according to QUANTITY_MODE:
       * 'delta' = existing.quantidade + incoming.quantidade (treat incoming as delta)
       * 'absolute' = existing.quantidade = incoming.quantidade
   - Other columns are left unchanged.
*/
async function syncProductsQuantityOnly(produtosRows, batch = 200) {
  for (let i = 0; i < produtosRows.length; i += batch) {
    const chunk = produtosRows.slice(i, i + batch);

    // normalize incoming
    for (const r of chunk) {
      if (!r.cliente_codigo || String(r.cliente_codigo).trim() === '') r.cliente_codigo = '0';
      r.quantidade = r.quantidade != null ? (Number(r.quantidade) || 0) : null;
      if (r.data_pedido) r.data_pedido = sanitizeDateValue(r.data_pedido);
      for (const k of Object.keys(r)) if (typeof r[k] === 'string' && isLikelyZeroDate(r[k])) r[k] = null;
    }

    const keys = Array.from(new Set(chunk.map(r => (r.produto_codigo||'').toString().trim()).filter(k => k !== '')));
    let existingMap = new Map(); // produto_codigo -> existing row (choose one if multiple)
    if (keys.length) {
      try {
        const { data: existing, error: selErr } = await supabase
          .from('import_clientes_produtos')
          .select('id,produto_codigo,quantidade')
          .in('produto_codigo', keys)
          .limit(10000);

        if (selErr) {
          console.error('Erro buscando produtos existentes (quantidade-only):', selErr);
        } else if (existing && existing.length) {
          // if duplicates exist, keep the one with largest id
          const grouped = {};
          for (const e of existing) {
            const k = (e.produto_codigo||'').toString().trim();
            if (!k) continue;
            if (!grouped[k]) grouped[k] = [];
            grouped[k].push(e);
          }
          for (const k of Object.keys(grouped)) {
            const arr = grouped[k];
            arr.sort((a,b) => (b.id||0) - (a.id||0));
            existingMap.set(k, arr[0]); // pick most recent id
          }
        }
      } catch (e) {
        console.error('Exception fetching existing products (quantity-only):', e);
      }
    }

    const inserts = [];
    const updates = []; // { id, newQuantidade }

    for (const incoming of chunk) {
      const code = (incoming.produto_codigo||'').toString().trim();
      const incomingQty = (incoming.quantidade === null || incoming.quantidade === undefined) ? null : Number(incoming.quantidade);
      if (!code) {
        // no code -> insert (can't match)
        inserts.push(incoming);
        continue;
      }
      const existing = existingMap.get(code);
      if (!existing) {
        // not found -> insert
        inserts.push(incoming);
      } else {
        // found -> compute new quantidade based on mode
        const existingQty = (existing.quantidade === null || existing.quantidade === undefined) ? 0 : Number(existing.quantidade) || 0;
        let newQty = existingQty;
        if (incomingQty === null) {
          // no quantity info -> skip update
          continue;
        } else {
          if (QUANTITY_MODE === 'delta') {
            newQty = existingQty + incomingQty;
          } else if (QUANTITY_MODE === 'absolute') {
            newQty = incomingQty;
          } else {
            // default to absolute if unknown mode
            newQty = incomingQty;
          }
        }
        // if changed, queue update
        if (Number(newQty) !== Number(existingQty)) {
          updates.push({ id: existing.id, quantidade: newQty });
        }
      }
    }

    // do inserts
    if (inserts.length) {
      try {
        const { error: insErr } = await supabase.from('import_clientes_produtos').insert(inserts, { returning: false });
        if (insErr) {
          console.error(`Insert chunk error (quantity-only) offset ${i}:`, insErr);
          // per-row fallback
          for (let r = 0; r < inserts.length; r++) {
            try {
              const { error: e2 } = await supabase.from('import_clientes_produtos').insert([inserts[r]], { returning: false });
              if (e2) console.error(`Row insert error import_clientes_produtos offset ${i} row ${r}:`, e2);
            } catch (er) {
              console.error(`Row insert thrown import_clientes_produtos offset ${i} row ${r}:`, er);
            }
          }
        } else {
          console.log(`Inserted ${inserts.length} new produtos (offset ${i}) [quantity-only]`);
        }
      } catch (e) {
        console.error('Exception inserting produtos chunk (quantity-only):', e);
      }
    }

    // do updates
    if (updates.length) {
      for (let u = 0; u < updates.length; u++) {
        const upd = updates[u];
        try {
          const { error: upErr } = await supabase.from('import_clientes_produtos').update({ quantidade: upd.quantidade }).eq('id', upd.id);
          if (upErr) {
            console.error(`Error updating produto id=${upd.id} quantidade=${upd.quantidade}:`, upErr);
          } else {
            // optional small log per update removed for verbosity
          }
        } catch (e) {
          console.error(`Exception updating produto id=${upd.id}:`, e);
        }
      }
      console.log(`Updated ${updates.length} produtos (offset ${i}) [quantity-only]`);
    }

    // small pause to avoid throttling
    await new Promise(res => setTimeout(res, 250));
  }
}

/* ---------------- deleteOrphansByKey (unchanged, optional) ---------------- */
async function deleteOrphansByKey(table, keyColumn, keepKeysSet, chunk = 1000) {
  if (!DO_DELETE_ORPHANS[table]) {
    console.log(`deleteOrphansByKey: skip for ${table} (flag disabled)`);
    return;
  }

  console.log(`deleteOrphansByKey: buscando chaves existentes em ${table} (${keyColumn})...`);
  let allExisting = [];
  let page = 0;
  const pageSize = 10000;
  while (true) {
    const { data, error } = await supabase
      .from(table)
      .select(keyColumn, { count: 'exact' })
      .range(page * pageSize, (page + 1) * pageSize - 1);
    if (error) {
      console.error(`Erro lendo chaves de ${table}:`, error);
      return;
    }
    if (!data || data.length === 0) break;
    allExisting = allExisting.concat(data.map(r => r[keyColumn]));
    if (data.length < pageSize) break;
    page++;
  }

  const toDelete = allExisting.filter(k => {
    if (k === null || k === undefined) return false;
    const ks = String(k).trim();
    if (ks === '') return false;
    return !keepKeysSet.has(ks);
  });

  console.log(`→ ${toDelete.length} registros serão deletados de ${table} (em chunks).`);
  for (let i = 0; i < toDelete.length; i += chunk) {
    const chunkArr = toDelete.slice(i, i + chunk);
    try {
      const { error } = await supabase.from(table).delete().in(keyColumn, chunkArr);
      if (error) console.error(`Erro ao deletar chunk em ${table} (offset ${i}):`, error);
      else console.log(`Deleted chunk ${i}-${i+chunkArr.length-1} from ${table}`);
    } catch (e) {
      console.error(`Exception during delete chunk in ${table} (offset ${i}):`, e);
    }
    await new Promise(res => setTimeout(res, 200));
  }

  console.log(`deleteOrphansByKey: concluído para ${table}`);
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

    const clientesArr = findArrayByHeuristics(json, ['clientes','lista_clientes','lista_clientes_geral','clientes_lista','clientes_data','users']);
    const clientesSource = Array.isArray(clientesArr) && clientesArr.length ? clientesArr : (Array.isArray(json) ? json : []);
    console.log(`→ Clientes no JSON: ${clientesSource.length}`);

    // clientes rows
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
      copy.criado_em = (new Date().toISOString()).replace(/\.\d+Z$/, 'Z');
      for (const k of Object.keys(copy)) if (typeof copy[k] === 'string' && isLikelyZeroDate(copy[k])) copy[k] = null;
      return copy;
    });

    clientesRows = clientesRows.map(c => {
      if (!c.codigo && c.cliente_codigo) c.codigo = c.cliente_codigo;
      return c;
    });

    clientesRows = (function dedupe(rows){ const map=new Map(); for(const r of rows){ const k=String(r.codigo||r.cliente_codigo||''); if(!map.has(k)) map.set(k,r);} return Array.from(map.values()); })(clientesRows);
    console.log(`→ DEDUPE / clientes únicos: ${clientesRows.length}`);

    // build sets / arrays
    const clientesKeys = new Set(clientesRows.map(c => String(c.codigo ?? c.cliente_codigo ?? '').trim()).filter(x => x !== ''));
    const pedidosRows = [];
    const produtosRows = [];

    /* extract embedded pedidos and produtos per client */
    for (let idx = 0; idx < clientesSource.length; idx++) {
      const client = clientesSource[idx] || {};
      const clienteCodigo = normalizeCodigo(client.codigo ?? client.cliente_codigo ?? client.id ?? '') || null;

      if (Array.isArray(client.pedidos)) {
        for (const rawItem of client.pedidos) {
          const p = pickFields(rawItem, COLUMNS_PEDIDOS);

          p.codigo_pedido = p.codigo_pedido ?? rawItem.codigo_pedido ?? rawItem.codigo ?? rawItem.numero_pedido ?? rawItem.order_id ?? rawItem.id ?? null;
          p.codigo_pedido = normalizeCodigo(p.codigo_pedido);

          p.cliente_codigo = normalizeCodigo(clienteCodigo ?? rawItem.cliente_codigo ?? rawItem.cliente ?? rawItem.codigo_cliente ?? rawItem.customer_id ?? rawItem.id_cliente) || '0';
          p.situacao_pedido = p.situacao_pedido ?? rawItem.status ?? rawItem.situacao ?? rawItem.status_pedido ?? null;

          p.data_hora_pedido = sanitizeDateValue(rawItem.data_hora_pedido ?? rawItem.data_pedido ?? rawItem.data_hora ?? rawItem.created_at ?? rawItem.criado_em ?? null);
          p.data_hora_confirmacao = sanitizeDateValue(rawItem.data_hora_confirmacao ?? rawItem.confirmado_em ?? rawItem.paid_at ?? null);

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

          for (const k of Object.keys(p)) if (typeof p[k] === 'string' && isLikelyZeroDate(p[k])) p[k] = null;

          if (!p.codigo_pedido) p.codigo_pedido = `P_${clienteCodigo || '0'}_${Math.floor(Math.random()*1e9)}`;

          pedidosRows.push(p);
        }
      }

      const produtosComprados = client.produtos_comprados;
      if (produtosComprados && typeof produtosComprados === 'object') {
        for (const key of Object.keys(produtosComprados)) {
          const prRaw = produtosComprados[key] || {};
          const produto_codigo = normalizeCodigo(prRaw.codigo ?? key) || null;
          const titulo = prRaw.titulo ?? prRaw.title ?? null;
          const parsed = {}; // not extracting size/color here, not needed
          const quantidade = prRaw.quantidade != null ? (parseInt(String(prRaw.quantidade).replace(/\D/g,''),10) || toNumberOrNull(prRaw.quantidade) || 0) : null;

          const produtoRow = {
            cliente_codigo: clienteCodigo || '0',
            produto_codigo,
            titulo,
            categoria_principal: prRaw.categoria_principal ?? prRaw.categoriaPrincipal ?? null,
            categoria: prRaw.categoria ?? null,
            marca: prRaw.marca ?? prRaw.brand ?? null,
            quantidade: quantidade,
            criado_em: new Date().toISOString(),
            tamanho: null,
            cor: null,
            sku: prRaw.sku ?? null,
            data_pedido: sanitizeDateValue(prRaw.data_pedido ?? prRaw.data)
          };

          for (const k of Object.keys(produtoRow)) if (typeof produtoRow[k] === 'string' && isLikelyZeroDate(produtoRow[k])) produtoRow[k] = null;

          produtosRows.push(produtoRow);
        }
      }
    }

    console.log(`→ EXTRAÍDO: pedidos ${pedidosRows.length}, produtos ${produtosRows.length}`);

    // placeholders
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
      for (let i = 0; i < placeholders.length; i += 300) {
        const chunk = placeholders.slice(i, i + 300);
        const { error } = await supabase.from('import_clientes').upsert(chunk, { onConflict: 'codigo' });
        if (error) console.error('Erro criando placeholders', error);
      }
      console.log('Placeholders processed.');
    } else {
      console.log('→ Nenhum placeholder necessário.');
    }

    // upsert clientes
    if (clientesRows.length) {
      await upsertClientesInBatches(clientesRows, 300);
    }
    // upsert pedidos
    if (pedidosRows.length) {
      await upsertPedidosInBatches(pedidosRows, 200);
    }
    // sync produtos (quantidade-only)
    if (produtosRows.length) {
      await syncProductsQuantityOnly(produtosRows, 200);
    }

    // optional delete orphans (disabled by default)
    try {
      await deleteOrphansByKey('import_clientes', 'codigo', clientesKeys, 1000);
      const pedidosKeysSet = new Set(pedidosRows.map(p => String(p.codigo_pedido).trim()).filter(x => x !== ''));
      const produtosKeysSet = new Set(produtosRows.map(p => String(p.produto_codigo||'').trim()).filter(x => x !== ''));
      await deleteOrphansByKey('import_pedidos', 'codigo_pedido', pedidosKeysSet, 1000);
      await deleteOrphansByKey('import_clientes_produtos', 'produto_codigo', produtosKeysSet, 1000);
    } catch (e) {
      console.error('Erro durante delete-orphans process:', e);
    }

    console.log("Sync finished successfully.");
  } catch (err) {
    console.error("Fatal error in main:", err);
    process.exit(1);
  }
}

process.on('unhandledRejection', (reason, p) => {
  console.error('Unhandled Rejection at:', p, 'reason:', reason);
  process.exit(1);
});

main().catch(e => {
  console.error("Fatal error", e);
  process.exit(1);
});
