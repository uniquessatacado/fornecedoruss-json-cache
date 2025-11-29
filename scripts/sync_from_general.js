/* scripts/sync_from_general.js */
const fs = require('fs');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing Supabase credentials");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false }
});

/* ----------------------------------------------------------
   COLUNAS
---------------------------------------------------------- */
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

/* ----------------------------------------------------------
   FUNÇÕES AUXILIARES
---------------------------------------------------------- */
function pickFields(obj, allowed) {
  const res = {};
  allowed.forEach(k => {
    if (obj.hasOwnProperty(k)) res[k] = obj[k];
  });
  return res;
}

function findArrayByHeuristics(json, names) {
  for (const name of names) {
    if (Array.isArray(json[name])) return json[name];
  }
  for (const k of Object.keys(json)) {
    if (Array.isArray(json[k])) return json[k];
  }
  return Array.isArray(json) ? json : [];
}

/* ----------------------------------------------------------
   NORMALIZAÇÃO DE DATAS
---------------------------------------------------------- */
function parseDateString(val) {
  if (!val) return null;
  const t = String(val).trim();

  // tenta ISO direto
  if (!isNaN(Date.parse(t))) return new Date(t).toISOString();

  const m = t.match(/^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/);
  if (!m) return null;

  const [ , dd, mm, yyyy, timeRaw ] = m;
  let time = timeRaw || "00:00:00";
  if (/^\d{2}:\d{2}$/.test(time)) time += ":00";

  return `${yyyy}-${mm}-${dd}T${time}Z`;
}

function normalizeDates(rows) {
  rows.forEach(r => {
    for (const k of Object.keys(r)) {
      const iso = parseDateString(r[k]);
      if (iso) r[k] = iso;
    }
  });
}

/* ----------------------------------------------------------
   DEDUPE POR 'codigo', MANTENDO O REGISTRO MAIS RECENTE
---------------------------------------------------------- */
function getDate(it) {
  const fields = ["criado_em","data_cadastro","data_hora_pedido"];
  for (const f of fields) {
    const iso = parseDateString(it[f]);
    if (iso) return new Date(iso);
  }
  return null;
}

function dedupeClientes(arr) {
  const map = new Map();

  for (const it of arr) {
    const codigo = String(it.codigo || it.cliente_codigo || "");

    if (!codigo) continue;

    const date = getDate(it);

    if (!map.has(codigo)) {
      map.set(codigo, { item: it, date });
    } else {
      const old = map.get(codigo);
      if (date && (!old.date || date > old.date)) {
        map.set(codigo, { item: it, date });
      }
    }
  }

  console.log("→ DEDUPE FINAL: ", map.size, "clientes únicos");
  return Array.from(map.values()).map(v => v.item);
}

/* ----------------------------------------------------------
   DELETE + INSERT EM LOTES
---------------------------------------------------------- */
async function clear(table) {
  await supabase.from(table).delete().gte("id", 0);
}

async function batchInsert(table, rows, size = 300) {
  for (let i = 0; i < rows.length; i += size) {
    const chunk = rows.slice(i, i + size);
    const { error } = await supabase.from(table).insert(chunk, { returning: false });
    if (error) throw error;
  }
}

/* ----------------------------------------------------------
   MAIN
---------------------------------------------------------- */
async function main() {
  const file = process.argv[2];
  if (!file || !fs.existsSync(file)) {
    console.error("Arquivo não encontrado:", file);
    process.exit(1);
  }

  const json = JSON.parse(fs.readFileSync(file, "utf8"));

  const clientesRaw = findArrayByHeuristics(json, [
    "clientes","lista_clientes","lista_clientes_geral",
    "clientes_lista","clientes_data","users"
  ]);

  const pedidosRaw = findArrayByHeuristics(json, [
    "pedidos","lista_pedidos","orders","lista_orders"
  ]);

  const produtosRaw = findArrayByHeuristics(json, [
    "produtos","itens","items","lista_produtos","order_items"
  ]);

  console.log("→ ORIGINAIS:");
  console.log("Clientes:", clientesRaw.length);
  console.log("Pedidos:", pedidosRaw.length);
  console.log("Produtos:", produtosRaw.length);

  // 🔥 aplicar dedupe
  const clientesClean = dedupeClientes(clientesRaw);

  const clientes = clientesClean.map(c => pickFields(c, COLUMNS_CLIENTES));
  const pedidos = pedidosRaw.map(p => pickFields(p, COLUMNS_PEDIDOS));
  const produtos = produtosRaw.map(p => pickFields(p, COLUMNS_PRODUTOS));

  normalizeDates(clientes);
  normalizeDates(pedidos);
  normalizeDates(produtos);

  console.log("→ LIMPAR TABELAS…");
  await clear("import_clientes");
  await clear("import_pedidos");
  await clear("import_clientes_produtos");

  console.log("→ INSERINDO…");

  await batchInsert("import_clientes", clientes);
  await batchInsert("import_pedidos", pedidos);
  await batchInsert("import_clientes_produtos", produtos);

  console.log("✔️ SYNC COMPLETO SEM ERROS");
}

main().catch(e => {
  console.error("FATAL:", e);
  process.exit(1);
});
