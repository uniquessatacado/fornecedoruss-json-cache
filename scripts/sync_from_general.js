/* scripts/sync_from_general.js
   Versão: 2025-11-29
   Função: sincronizar JSON "general" para 3 tabelas:
     - import_clientes (UPsert por `codigo`)
     - import_pedidos (insert em lote)
     - import_clientes_produtos (insert em lote)
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

/* ------------------ colunas esperadas (mapear apenas essas) ------------------ */
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

function parseDateString(val) {
  if (!val || typeof val !== "string") return null;
  const s = val.trim();
  // Match DD/MM/YYYY or DD/MM/YYYY HH:MM[:SS]
  const m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/);
  if (!m) return null;
  const day = m[1], month = m[2], year = m[3];
  let time = m[4] || "00:00:00";
  if (/^\d{2}:\d{2}$/.test(time)) time = time + ":00";
  return `${year}-${month}-${day}T${time}Z`;
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

function toNumberOrNull(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === 'number') return v;
  const s = String(v).replace(/[^\d\-.,]/g, '').trim();
  if (s === '') return null;
  // replace comma decimal to dot
  const t = s.replace(/\./g, '').replace(/,/g, '.');
  const n = parseFloat(t);
  return Number.isFinite(n) ? n : null;
}

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
    const k = keys.map(k0 => (r[k0] === undefined || r[k0] === null) ? '' : String(r[k0])).join('|');
    if (!map.has(k)) map.set(k, r); // mantém primeiro
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

  // pick only columns we care about
  let clientesRows = clientesArr.map(it => pickFields(it, COLUMNS_CLIENTES));
  let pedidosRows  = pedidosArr.map(it => pickFields(it, COLUMNS_PEDIDOS));
  let produtosRows = produtosArr.map(it => pickFields(it, COLUMNS_PRODUTOS));

  // normalize dates
  normalizeDatesInRows(clientesRows);
  normalizeDatesInRows(pedidosRows);
  normalizeDatesInRows(produtosRows);

  // normalize codigo / cliente_codigo: trim, remove zeros à esquerda, forçar strings
  clientesRows = clientesRows.map(r => {
    const copy = { ...r };
    if (copy.codigo !== undefined && copy.codigo !== null) {
      copy.codigo = String(copy.codigo).trim();
      copy.codigo = copy.codigo.replace(/[^0-9a-zA-Z\-_.]/g, ''); // remove espaços e chars estranhos
      copy.codigo = copy.codigo.replace(/^0+/, ''); // remove leading zeros (se quiser manter zeros, comente)
      if (copy.codigo === '') copy.codigo = null;
    }
    // manter cliente_codigo igual a codigo se ausente
    if ((copy.cliente_codigo === undefined || copy.cliente_codigo === null || String(copy.cliente_codigo).trim() === '') && copy.codigo) {
      copy.cliente_codigo = copy.codigo;
    } else if (copy.cliente_codigo !== undefined && copy.cliente_codigo !== null) {
      copy.cliente_codigo = String(copy.cliente_codigo).trim().replace(/^0+/, '');
      if (copy.cliente_codigo === '') copy.cliente_codigo = null;
    }
    return copy;
  });

  // small normalization for pedidos/produtos numbers (optional)
  pedidosRows = pedidosRows.map(r => {
    const copy = { ...r };
    if (copy.cliente_codigo !== undefined && copy.cliente_codigo !== null) {
      copy.cliente_codigo = String(copy.cliente_codigo).trim().replace(/^0+/, '') || null;
    }
    return copy;
  });

  produtosRows = produtosRows.map(r => {
    const copy = { ...r };
    if (copy.cliente_codigo !== undefined && copy.cliente_codigo !== null) {
      copy.cliente_codigo = String(copy.cliente_codigo).trim().replace(/^0+/, '') || null;
    }
    if (copy.quantidade !== undefined && copy.quantidade !== null) {
      const q = parseInt(String(copy.quantidade).replace(/\D/g, ''), 10);
      copy.quantidade = Number.isFinite(q) ? q : null;
    }
    return copy;
  });

  // diagnóstico duplicatas
  const dupDiag = detectDuplicates(clientesRows, ['codigo']);
  console.log(`→ DUPUPE DIAG BEFORE (clientes) : ${dupDiag.count}`);
  if (dupDiag.examples.length) console.log('→ exemplos duplicatas (antes):', JSON.stringify(dupDiag.examples.slice(0,6), null, 2));

  // dedupe mantendo a primeira ocorrência por 'codigo'
  clientesRows = dedupeByKey(clientesRows, ['codigo']);
  console.log(`→ DEDUPE FINAL: ${clientesRows.length} clientes únicos (orig ${clientesArr.length})`);

  console.log("→ LIMPAR TABELAS...");

  // sincronizar import_clientes com upsert (evita duplicate key)
  try {
    await deleteAll('import_clientes');
    if (clientesRows.length) {
      const batch = 300;
      for (let i = 0; i < clientesRows.length; i += batch) {
        const chunk = clientesRows.slice(i, i + batch);
        // Upsert por 'codigo' (mantenha 'codigo' como onConflict — ajuste se seu unique for outro)
        const { error } = await supabase
          .from('import_clientes')
          .upsert(chunk, { onConflict: 'codigo' });
        if (error) {
          console.error(`Erro em upsert import_clientes (offset ${i})`, error);
          // log exemplo da primeira linha do chunk para debug
          console.error('exemplo linha:', JSON.stringify(chunk[0], null, 2));
          throw error;
        }
        console.log(`Upserted ${chunk.length} into import_clientes (offset ${i})`);
      }
    }
  } catch (e) {
    console.error("FATAL ao inserir import_clientes:", e);
    throw e;
  }

  // sincronizar import_pedidos (clear + insert)
  try {
    await deleteAll('import_pedidos');
    if (pedidosRows.length) {
      await insertInBatches('import_pedidos', pedidosRows);
    }
  } catch (e) {
    console.error("FATAL ao inserir import_pedidos:", e);
    throw e;
  }

  // sincronizar import_clientes_produtos (clear + insert)
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
