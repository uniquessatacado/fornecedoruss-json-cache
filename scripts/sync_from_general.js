/* scripts/sync_from_general.js
   Versão com dedupe + diagnóstico + normalização de tipos e datas
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

/* --------------------------------------------------------------
   1. FORMATOS DAS COLUNAS (campos que queremos manter)
-------------------------------------------------------------- */
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

/* --------------------------------------------------------------
   2. FUNÇÕES AUXILIARES
-------------------------------------------------------------- */
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

/* --------------------------------------------------------------
   3. NORMALIZAÇÃO DE DATAS (DD/MM/YYYY -> ISO) e DETECÇÃO
-------------------------------------------------------------- */
function parseDateString(val) {
  if (!val || typeof val !== "string") return null;
  const s = val.trim();

  // Match DD/MM/YYYY ou DD/MM/YYYY HH:MM[:SS]
  const m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/);
  if (!m) return null;

  const day = m[1];
  const month = m[2];
  const year = m[3];
  let time = m[4] || "00:00:00";

  if (/^\d{2}:\d{2}$/.test(time)) time = time + ":00";

  // Devolve ISO sem ajuste de timezone (Z): aceitável para Postgres timestamp
  return `${year}-${month}-${day}T${time}Z`;
}

function isISODateString(s) {
  return typeof s === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(s);
}

function normalizeDatesInRows(rows) {
  if (!Array.isArray(rows)) return;

  const dateRegex = /^\d{2}\/\d{2}\/\d{4}/; // dd/mm/yyyy

  rows.forEach(row => {
    if (!row || typeof row !== "object") return;

    for (const key of Object.keys(row)) {
      const v = row[key];
      if (typeof v === "string" && dateRegex.test(v.trim())) {
        const iso = parseDateString(v);
        if (iso) row[key] = iso;
      }
    }
  });
}

/* --------------------------------------------------------------
   4. NORMALIZAÇÃO DE TIPOS (int/float/null) - heurística
-------------------------------------------------------------- */
function tryParseInt(val) {
  if (val === null || val === undefined) return null;
  if (typeof val === 'number' && Number.isInteger(val)) return val;
  const s = String(val).replace(/\D+$/,''); // remove non-digits trailing (conservador)
  if (/^-?\d+$/.test(s)) return parseInt(s, 10);
  return null;
}

function tryParseFloat(val) {
  if (val === null || val === undefined) return null;
  if (typeof val === 'number') return val;
  const s = String(val).replace(/\s/g,'').replace(/[^0-9\.,-]+/g,'').replace(',','.');
  const n = parseFloat(s);
  return isNaN(n) ? null : n;
}

function normalizeRowTypes(row) {
  if (!row || typeof row !== 'object') return row;

  const out = {};
  for (const k of Object.keys(row)) {
    let v = row[k];

    // limpar strings vazias
    if (typeof v === 'string' && v.trim() === '') {
      out[k] = null;
      continue;
    }

    // Se já for uma ISO-like data, mantenha
    if (typeof v === 'string' && isISODateString(v.trim())) {
      out[k] = v.trim();
      continue;
    }

    // campos que são id/codigo => preferência por inteiro (se possível)
    if (/id$|^id_|codigo|_codigo|cliente_codigo/i.test(k)) {
      const asInt = tryParseInt(v);
      out[k] = asInt !== null ? asInt : (v === null ? null : String(v));
      continue;
    }

    // campos de valor/total/percentual => float
    if (/valor|total|preco|percentual|frete|desconto/i.test(k)) {
      const asNum = tryParseFloat(v);
      out[k] = asNum !== null ? asNum : (v === null ? null : v);
      continue;
    }

    // campos que terminam com _em / data / criado / data_pedido => manter ISO se possível
    if (/data|_em|criado|data_pedido|data_hora|hora|created_at/i.test(k) && typeof v === 'string') {
      // tenta parsear DD/MM/YYYY
      const iso = parseDateString(v);
      if (iso) {
        out[k] = iso;
        continue;
      }
    }

    // fallback: manter como veio, convertendo strings longas a trim
    if (typeof v === 'string') out[k] = v.trim();
    else out[k] = v;
  }

  return out;
}

/* --------------------------------------------------------------
   5. DEDUPE (normalização de chave + diagnóstico)
-------------------------------------------------------------- */
function normalizeKeyVal(val) {
  if (val === null || val === undefined) return '__NULL__';
  let s = String(val);
  s = s.trim();
  s = s.replace(/\s+/g, ' ');
  s = s.replace(/^0+/, ''); // remove zeros à esquerda
  s = s.toLowerCase();
  return s === '' ? '__EMPTY__' : s;
}

function detectDuplicates(rows, keyPriority = ['codigo','cliente_codigo']) {
  const seen = new Map();
  const dupExamples = [];
  for (const r of rows) {
    let rawVal = null;
    for (const k of keyPriority) {
      if (r[k] !== undefined && r[k] !== null && String(r[k]).trim() !== '') {
        rawVal = r[k];
        break;
      }
    }
    const k = normalizeKeyVal(rawVal);
    if (seen.has(k)) {
      if (dupExamples.length < 12) {
        dupExamples.push({
          keyTried: keyPriority,
          raw: rawVal,
          normalized: k,
          first: seen.get(k),
          current: r
        });
      }
    } else {
      seen.set(k, r);
    }
  }
  return {count: dupExamples.length, examples: dupExamples};
}

function dedupeByKey(rows, keyPriority = ['codigo','cliente_codigo']) {
  const seen = new Set();
  const out = [];
  for (const r of rows) {
    let rawVal = null;
    for (const k of keyPriority) {
      if (r[k] !== undefined && r[k] !== null && String(r[k]).trim() !== '') {
        rawVal = r[k];
        break;
      }
    }
    const kNorm = normalizeKeyVal(rawVal);
    if (seen.has(kNorm)) continue;
    seen.add(kNorm);
    out.push(r);
  }
  return out;
}

/* --------------------------------------------------------------
   6. DELETE + INSERT EM LOTES
-------------------------------------------------------------- */
async function deleteAll(table) {
  try {
    const { error } = await supabase.from(table).delete().gt('id', 0);
    if (error) {
      console.log(`delete fallback for ${table}`, error.message || error);
      await supabase.from(table).delete().not('id', 'is', null);
    } else {
      console.log(`Deleted contents of ${table}`);
    }
  } catch(e) {
    console.error("delete error", e);
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

/* --------------------------------------------------------------
   7. MAIN (fluxo principal)
-------------------------------------------------------------- */
async function main() {
  const source = process.argv[2];

  if (!source || !fs.existsSync(source)) {
    console.error("Arquivo não encontrado:", source);
    process.exit(1);
  }

  const raw = fs.readFileSync(source, "utf8");

  let json;
  try {
    json = JSON.parse(raw);
  } catch (e) {
    console.error("Erro parseando JSON:", e.message);
    process.exit(1);
  }

  const clientesArr = findArrayByHeuristics(json, [
    "clientes","lista_clientes","lista_clientes_geral",
    "clientes_lista","clientes_data","users"
  ]) || [];

  const pedidosArr = findArrayByHeuristics(json, [
    "pedidos","lista_pedidos","orders","lista_orders","pedidos_lista"
  ]) || [];

  const produtosArr = findArrayByHeuristics(json, [
    "produtos","itens","items","lista_produtos","order_items"
  ]) || [];

  console.log(`→ ORIGINAIS:`);
  console.log(`Clientes: ${clientesArr.length}`);
  console.log(`Pedidos: ${pedidosArr.length}`);
  console.log(`Produtos: ${produtosArr.length}`);

  // mapear apenas campos desejados
  let clientesRows = clientesArr.map(it => pickFields(it, COLUMNS_CLIENTES));
  let pedidosRows  = pedidosArr.map(it => pickFields(it, COLUMNS_PEDIDOS));
  let produtosRows = produtosArr.map(it => pickFields(it, COLUMNS_PRODUTOS));

  // normalizar datas brutas (DD/MM/YYYY)
  normalizeDatesInRows(clientesRows);
  normalizeDatesInRows(pedidosRows);
  normalizeDatesInRows(produtosRows);

  // diagnóstico antes do dedupe
  const diagBefore = detectDuplicates(clientesRows, ['codigo','cliente_codigo']);
  console.log(`→ DUPUPE DIAG BEFORE (clientes) : ${diagBefore.count}`);
  if (diagBefore.examples && diagBefore.examples.length) {
    console.log('→ exemplos duplicatas (antes):', JSON.stringify(diagBefore.examples.slice(0,5), null, 2));
  }

  // dedupe clientes
  const beforeCount = clientesRows.length;
  clientesRows = dedupeByKey(clientesRows, ['codigo','cliente_codigo']);
  console.log(`→ DEDUPE FINAL: ${clientesRows.length} clientes únicos (orig ${beforeCount})`);

  // aplicar normalização de tipo por linha (int/float/date)
  clientesRows = clientesRows.map(normalizeRowTypes);
  pedidosRows  = pedidosRows.map(normalizeRowTypes);
  produtosRows = produtosRows.map(normalizeRowTypes);

  // DEDUPE simples para pedidos/produtos (opcional)
  // aqui apenas garante array único por codigo para reduzir conflito
  // (se quiser lógica mais complexa, posso ajustar)
  // pedidosRows = dedupeByKey(pedidosRows, ['codigo_pedido','id']);
  // produtosRows = dedupeByKey(produtosRows, ['produto_codigo','id']);

  console.log("→ WILL CLEAR tables and reinsert.");

  // operações no banco
  await deleteAll('import_clientes');
  if (clientesRows.length) await insertInBatches('import_clientes', clientesRows);

  await deleteAll('import_pedidos');
  if (pedidosRows.length) await insertInBatches('import_pedidos', pedidosRows);

  await deleteAll('import_clientes_produtos');
  if (produtosRows.length) await insertInBatches('import_clientes_produtos', produtosRows);

  console.log("Sync finished successfully.");
}

main().catch(e => {
  console.error("Fatal error", e);
  process.exit(1);
});
