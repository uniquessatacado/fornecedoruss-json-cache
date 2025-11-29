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

const COLUMNS_CLIENTES = ['cliente_codigo','codigo','nome','email','data_cadastro','whatsapp','cidade','estado','loja_drop','representante','total_pedidos','valor_total_comprado','criado_em'];
const COLUMNS_PEDIDOS = ['id','codigo_pedido','cliente_codigo','situacao_pedido','data_hora_pedido','data_hora_confirmacao','valor_total_produtos','valor_frete','frete','valor_total_pedido','desconto','cidade','estado','percentual_comissao','origem_pedido','tipo_compra','texto_tipo_compra','pedidos_loja_drop','criado_em'];
const COLUMNS_PRODUTOS = ['id','cliente_codigo','produto_codigo','titulo','categoria_principal','categoria','marca','quantidade','criado_em','id_pedido','valor_unitario','subcategoria','tamanho','cor','sku','data_pedido'];

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
    const { error } = await supabase.from(table).insert(chunk, { returning: false });
    if (error) {
      console.error(`Error inserting into ${table} (offset ${i})`, error);
      throw error;
    }
    console.log(`Inserted ${chunk.length} into ${table} (offset ${i})`);
  }
}

async function main() {
  const source = process.argv[2];
  if (!source || !fs.existsSync(source)) {
    console.error("Arquivo não encontrado:", source);
    process.exit(1);
  }
  const raw = fs.readFileSync(source, 'utf8');
  let json;
  try { json = JSON.parse(raw); } catch(e) { console.error("Erro parseando JSON:", e.message); process.exit(1); }

  const clientesCandidates = ['clientes','lista_clientes','lista_clientes_geral','clientes_lista','clientes_data','users'];
  const pedidosCandidates = ['pedidos','lista_pedidos','orders','lista_orders','pedidos_lista'];
  const produtosCandidates = ['produtos','itens','items','lista_produtos','order_items'];

  const clientesArr = findArrayByHeuristics(json, clientesCandidates) || [];
  const pedidosArr = findArrayByHeuristics(json, pedidosCandidates) || [];
  const produtosArr = findArrayByHeuristics(json, produtosCandidates) || [];

  console.log(`Detected arrays sizes -> clientes: ${clientesArr.length}, pedidos: ${pedidosArr.length}, produtos: ${produtosArr.length}`);

 const clientesRows = clientesArr.map(it => (pickFields(it, COLUMNS_CLIENTES)));
const pedidosRows  = pedidosArr.map(it => (pickFields(it, COLUMNS_PEDIDOS)));
const produtosRows = produtosArr.map(it => (pickFields(it, COLUMNS_PRODUTOS)));


  console.log("WILL CLEAR tables and reinsert.");

  await deleteAll('import_clientes');
  if (clientesRows.length) await insertInBatches('import_clientes', clientesRows);

  await deleteAll('import_pedidos');
  if (pedidosRows.length) await insertInBatches('import_pedidos', pedidosRows);

  await deleteAll('import_clientes_produtos');
  if (produtosRows.length) await insertInBatches('import_clientes_produtos', produtosRows);

  console.log("Sync finished successfully.");
}

main().catch(e => { console.error("Fatal error", e); process.exit(1); });
