/* scripts/sync_from_general.js
   Versão final ajustada — preenche cliente_codigo em pedidos quando ausente (fallback = 0)
   2025-11-29
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

/* ------------------ colunas alvo ------------------ */
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

/* ------------------ helpers ------------------ */
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

/* ------------------ date helpers ------------------ */
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

function sanitizeDateValue(v, keyName) {
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

function normalizeDatesInRows(rows) {
  if (!Array.isArray(rows)) return;
  const dateRegex = /^\d{2}\/\d{2}\/\d{4}/;
  rows.forEach(row => {
    if (!row || typeof row !== 'object') return;
    for (const key of Object.keys(row)) {
      const v = row[key];
      if (typeof v === 'string' && dateRegex.test(v.trim())) {
        const iso = parseDateString(v);
        if (iso) row[key] = iso;
      }
    }
  });
}

/* ------------------ normalize / numeric helpers ------------------ */
function normalizeCodigo(val) {
  if (val === null || val === undefined) return null;
  let s = String(val).trim();
  s = s.replace(/[^0-9a-zA-Z\-_.]/g, '');
  s = s.replace(/^0+/, '');
  if (s === '') return null;
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

/* ------------------ dedupe helpers ------------------ */
function detectDuplicates(rows, keys = ['codigo']) {
  const seen = new Map();
  const examples = [];
  let count = 0;
  rows.forEach(r => {
    const k = keys.map(k0 => (r[k0] === undefined || r[k0] === null) ? '' : String(r[k0])).join('|');
    if (seen.has(k)) {
      count++;
      if (examples.length < 10) examples.push({ key: k, first: seen.get(k), dup: r });
    } else {
      seen.set(k, r);
    }
  });
  return { count, examples };
}

function dedupeByKey(rows, keys = ['codigo']) {
  const map = new Map();
  for (const r of rows) {
    const key = keys.map(k0 => (r[k0] === undefined || r[k0] === null) ? '' : String(r[k0])).join('|');
    if (!map.has(key)) map.set(key, r);
  }
  return Array.from(map.values());
}

/* ------------------ db helpers ------------------ */
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

async function insertInBatches(table, rows, batch = 300) {
  for (let i = 0; i < rows.length; i += batch) {
    const chunk = rows.slice(i, i + batch);
    if (chunk.length === 0) continue;
    const { error } = await supabase.from(table).insert(chunk, { returning: false });
    if (error) {
      console.error(`Error inserting into ${table} (offset ${i})`, error);
      throw error;
    }
    console.log(`Inserted ${chunk.length} into ${table} (offset ${i})`);
  }
}

/* ------------------ MAIN ------------------ */
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

  // pick fields
  let clientesRows = clientesArr.map(it => pickFields(it, COLUMNS_CLIENTES));
  let pedidosRows  = pedidosArr.map(it => pickFields(it, COLUMNS_PEDIDOS));
  let produtosRows = produtosArr.map(it => pickFields(it, COLUMNS_PRODUTOS));

  // normalize dates
  normalizeDatesInRows(clientesRows);
  normalizeDatesInRows(pedidosRows);
  normalizeDatesInRows(produtosRows);

  // normalize codigo / cliente_codigo and basic numeric casts
  clientesRows = clientesRows.map(row => {
    const r = { ...row };
    r.codigo = (r.codigo !== undefined && r.codigo !== null) ? normalizeCodigo(r.codigo) : null;
    if (!r.cliente_codigo && r.codigo) r.cliente_codigo = r.codigo;
    else if (r.cliente_codigo) r.cliente_codigo = normalizeCodigo(r.cliente_codigo);
    if (r.total_pedidos !== undefined) r.total_pedidos = parseInt(String(r.total_pedidos).replace(/\D/g, ''), 10) || null;
    if (r.valor_total_comprado !== undefined) r.valor_total_comprado = toNumberOrNull(r.valor_total_comprado);
    return r;
  });

  pedidosRows = pedidosRows.map(row => {
    const r = { ...row };
    if (r.cliente_codigo !== undefined && r.cliente_codigo !== null) r.cliente_codigo = normalizeCodigo(r.cliente_codigo);
    if (r.valor_total_produtos !== undefined) r.valor_total_produtos = toNumberOrNull(r.valor_total_produtos);
    if (r.valor_total_pedido !== undefined) r.valor_total_pedido = toNumberOrNull(r.valor_total_pedido);
    // ensure date fields sanitized
    if (r.data_hora_pedido) r.data_hora_pedido = sanitizeDateValue(r.data_hora_pedido, 'data_hora_pedido');
    if (r.data_hora_confirmacao) r.data_hora_confirmacao = sanitizeDateValue(r.data_hora_confirmacao, 'data_hora_confirmacao');
    return r;
  });

  produtosRows = produtosRows.map(row => {
    const r = { ...row };
    if (r.cliente_codigo !== undefined && r.cliente_codigo !== null) r.cliente_codigo = normalizeCodigo(r.cliente_codigo);
    if (r.quantidade !== undefined) r.quantidade = parseInt(String(r.quantidade).replace(/\D/g, ''), 10) || null;
    if (r.valor_unitario !== undefined) r.valor_unitario = toNumberOrNull(r.valor_unitario);
    if (r.data_pedido) r.data_pedido = sanitizeDateValue(r.data_pedido, 'data_pedido');
    return r;
  });

  // diagnóstico duplicatas
  const dupDiag = detectDuplicates(clientesRows, ['codigo']);
  console.log(`→ DUPUPE DIAG BEFORE (clientes) : ${dupDiag.count}`);
  if (dupDiag.examples && dupDiag.examples.length) console.log('→ exemplos duplicatas (antes):', JSON.stringify(dupDiag.examples.slice(0,6), null, 2));

  // dedupe por codigo (mantém primeiro)
  clientesRows = dedupeByKey(clientesRows, ['codigo']);
  console.log(`→ DEDUPE FINAL: ${clientesRows.length} clientes únicos (orig ${clientesArr.length})`);

  console.log("→ LIMPAR TABELAS...");

  // Upsert import_clientes with sanitization of dates
  try {
    await deleteAll('import_clientes');
    if (clientesRows.length) {
      const batch = 300;
      for (let i = 0; i < clientesRows.length; i += batch) {
        const rawChunk = clientesRows.slice(i, i + batch);

        const chunk = rawChunk.map(row => {
          const copy = { ...row };
          for (const k of Object.keys(copy)) {
            const v = copy[k];
            if (v === null || v === undefined) continue;
            if (typeof v === 'string' && isLikelyZeroDate(v)) { copy[k] = null; continue; }
            if (/data|criado|hora|date|timestamp/i.test(k)) {
              const sd = sanitizeDateValue(v, k);
              copy[k] = sd;
              continue;
            }
          }
          if ((!copy.cliente_codigo || String(copy.cliente_codigo).trim() === '') && copy.codigo) copy.cliente_codigo = copy.codigo;
          return copy;
        });

        const badBefore = rawChunk.find(r => Object.values(r).some(v => typeof v === 'string' && v.includes('0000-00-00')));
        if (badBefore) {
          console.log(`DEBUG: linha com "0000-00-00" no offset ${i}:`, JSON.stringify(badBefore));
        }

        const { error } = await supabase
          .from('import_clientes')
          .upsert(chunk, { onConflict: 'codigo' });

        if (error) {
          console.error(`Erro em upsert import_clientes (offset ${i})`, error);
          console.error('exemplo linha (após sanitização):', JSON.stringify(chunk[0], null, 2));
          throw error;
        }
        console.log(`Upserted ${chunk.length} into import_clientes (offset ${i})`);
      }
    }
  } catch (e) {
    console.error("FATAL ao inserir import_clientes:", e);
    throw e;
  }

  // --- NEW: ensure pedidos have cliente_codigo non-nullable (fallback to 0)
  let fallbackCount = 0;
  const fallbackExamples = [];
  pedidosRows = pedidosRows.map((p, idx) => {
    const copy = { ...p };
    if (copy.cliente_codigo === undefined || copy.cliente_codigo === null || String(copy.cliente_codigo).trim() === '') {
      // fallback numeric 0 to satisfy NOT NULL constraint; log examples
      copy.cliente_codigo = 0;
      fallbackCount++;
      if (fallbackExamples.length < 5) fallbackExamples.push({ index: idx, sample: copy });
    }
    return copy;
  });
  if (fallbackCount > 0) {
    console.log(`→ WARN: ${fallbackCount} pedidos faltavam cliente_codigo — preenchidos com 0 (filtro NOT NULL). Exemplos:`, JSON.stringify(fallbackExamples, null, 2));
  }

  // Insert pedidos
  try {
    await deleteAll('import_pedidos');
    if (pedidosRows.length) {
      await insertInBatches('import_pedidos', pedidosRows);
    }
  } catch (e) {
    console.error("FATAL ao inserir import_pedidos:", e);
    throw e;
  }

  // Insert produtos
  try {
    await deleteAll('import_clientes_produtos');
    if (produtosRows.length) {
      await insertInBatches('import_clientes_produtos', produtosRows);
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
