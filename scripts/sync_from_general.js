/* scripts/sync_from_general.js */
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
   1. FORMATOS DAS COLUNAS (ajuste os nomes se necessário)
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
   1.1 Tipos esperados (colunas inteiras, numéricas e datas)
   Ajuste listas se a sua tabela tiver colunas diferentes.
-------------------------------------------------------------- */
const INT_COLUMNS = new Set([
  'id','cliente_codigo','quantidade','id_pedido','loja_drop','representante','total_pedidos'
]);

const NUMERIC_COLUMNS = new Set([
  'valor_total_comprado','valor_total_produtos','valor_frete','valor_total_pedido',
  'desconto','percentual_comissao','valor_unitario'
]);

const DATE_COLUMNS = new Set([
  'data_cadastro','criado_em','data_hora_pedido','data_hora_confirmacao','data_pedido'
]);

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
   3. PARSE E NORMALIZAÇÃO DE DATAS DD/MM/YYYY -> ISO
-------------------------------------------------------------- */
function parseDateString(val) {
  if (!val || typeof val !== "string") return null;
  const s = val.trim();

  // Match DD/MM/YYYY ou DD/MM/YYYY HH:MM[:SS]
  const m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/);
  if (!m) return null;

  const day = m[1], month = m[2], year = m[3];
  let time = m[4] || "00:00:00";
  if (/^\d{2}:\d{2}$/.test(time)) time = time + ":00";

  // Retorna ISO (Z para indicar UTC; ajuste se preferir sem Z)
  return `${year}-${month}-${day}T${time}Z`;
}

function tryParseIsoIfDateString(val) {
  // se já for ISO válido, retorna como está
  if (typeof val !== 'string') return val;
  const isoMatch = val.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/);
  if (isoMatch) return val;
  const parsed = parseDateString(val);
  return parsed || val;
}

/* --------------------------------------------------------------
   4. COERÇÃO/CONVERSÃO DE TIPOS POR LINHA
-------------------------------------------------------------- */
function coerceRowTypes(row) {
  if (!row || typeof row !== 'object') return row;

  const out = {};
  for (const [k, v] of Object.entries(row)) {
    if (v === null || v === undefined || (typeof v === 'string' && v.trim() === '')) {
      out[k] = null;
      continue;
    }

    // datas
    if (DATE_COLUMNS.has(k)) {
      // se já é timestamp/iso, mantém; se for DD/MM/YYYY converte
      const maybeIso = tryParseIsoIfDateString(String(v));
      out[k] = maybeIso;
      continue;
    }

    // inteiros
    if (INT_COLUMNS.has(k)) {
      // remover espaços, tirar possíveis formatações
      const n = parseInt(String(v).replace(/[^\d-]/g, ''), 10);
      out[k] = Number.isNaN(n) ? null : n;
      continue;
    }

    // numéricos
    if (NUMERIC_COLUMNS.has(k)) {
      const cleaned = String(v).replace(/[^\d,.-]/g,"").replace(/\./g,"").replace(/,/g,".");
      const f = parseFloat(cleaned);
      out[k] = Number.isNaN(f) ? null : f;
      continue;
    }

    // default: manter string/texto
    out[k] = v;
  }

  return out;
}

/* --------------------------------------------------------------
   5. DELETE + INSERT EM LOTES
-------------------------------------------------------------- */
async function deleteAll(table) {
  try {
    const { error } = await supabase.from(table).delete().gt('id', 0);
    if (error) {
      console.log(`delete fallback for ${table}`, error.message);
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

    const { error } = await supabase
      .from(table)
      .insert(chunk, { returning: false });

    if (error) {
      console.error(`Error inserting into ${table} (offset ${i})`, error);
      throw error;
    }

    console.log(`Inserted ${chunk.length} into ${table} (offset ${i})`);
  }
}

/* --------------------------------------------------------------
   6. DEDUPE helpers
-------------------------------------------------------------- */
function dedupeByKey(rows, key) {
  const seen = new Set();
  const out = [];
  for (const r of rows) {
    const val = r[key] ?? null;
    const k = (val === null || val === undefined) ? '__NULL__' : String(val);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(r);
  }
  return out;
}

/* --------------------------------------------------------------
   7. MAIN
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

  console.log(
    `→ ORIGINAIS:\nClientes: ${clientesArr.length}\nPedidos: ${pedidosArr.length}\nProdutos: ${produtosArr.length}`
  );

  // mapear campos e aplicar coerção de tipos
  let clientesRows = clientesArr.map(it => coerceRowTypes(pickFields(it, COLUMNS_CLIENTES)));
  let pedidosRows  = pedidosArr.map(it => coerceRowTypes(pickFields(it, COLUMNS_PEDIDOS)));
  let produtosRows = produtosArr.map(it => coerceRowTypes(pickFields(it, COLUMNS_PRODUTOS)));

  // dedupe por codigo de cliente (evita duplicate key)
  if (clientesRows.length) {
    const before = clientesRows.length;
    clientesRows = dedupeByKey(clientesRows, 'codigo');
    console.log(`→ DEDUPE FINAL: ${clientesRows.length} clientes únicos (orig ${before})`);
  }

  console.log("→ LIMPAR TABELAS...");

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
