/* scripts/sync_from_general.js
   Uso:
     node ./scripts/sync_from_general.js /tmp/source_general.json
   Observações:
     - Deve ter SUPABASE_URL e SUPABASE_KEY no ambiente (service role).
     - Faz upsert por 'codigo' em clientes, 'codigo_pedido' em pedidos, e 'produto_codigo' em produtos.
     - Gera arquivos de debug em C:\temp\ para linhas problemáticas.
*/

const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing Supabase credentials (SUPABASE_URL / SUPABASE_KEY)");
  process.exit(1);
}
const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });

/* Colunas esperadas (ajuste se necessário) */
const COLUMNS_CLIENTES = ['cliente_codigo','codigo','nome','email','data_cadastro','whatsapp','cidade','estado','loja_drop','representante','total_pedidos','valor_total_comprado','criado_em'];
const COLUMNS_PEDIDOS  = ['id','codigo_pedido','cliente_codigo','situacao_pedido','data_hora_pedido','data_hora_confirmacao','data_hora_confirmacao_pagamento','valor_total_produtos','valor_frete','frete','valor_total_pedido','desconto','cidade','estado','percentual_comissao','origem_pedido','tipo_compra','texto_tipo_compra','pedidos_loja_drop','criado_em'];
const COLUMNS_PRODUTOS = ['id','cliente_codigo','produto_codigo','titulo','categoria_principal','categoria','marca','quantidade','criado_em','id_pedido','valor_unitario','subcategoria','tamanho','cor','sku','data_pedido'];

/* ---------------- utilitários ---------------- */
function safeString(v) {
  if (v === undefined || v === null) return null;
  return String(v).trim();
}
function isZeroDateIso(s) {
  if (!s) return false;
  return /^0{4}-0{2}-0{2}/.test(String(s));
}
function parseDateToIso(v) {
  if (!v) return null;
  const s = String(v).trim();
  // dd/mm/yyyy or dd\/mm\/yyyy
  const ddmmy = /^(\d{2})[\/\-](\d{2})[\/\-](\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/;
  const m = s.match(ddmmy);
  if (m) {
    const d = m[1], mo = m[2], y = m[3], time = m[4] || "00:00:00";
    const t = time.length === 5 ? time + ":00" : time;
    return `${y}-${mo}-${d}T${t}Z`;
  }
  // Already ISO-ish: 2023-10-16 22:43:38  -> convert space to T and append Z
  const isoLike = /^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?$/;
  if (isoLike.test(s)) {
    const parts = s.replace(' ', 'T');
    if (!/\d{2}:\d{2}:\d{2}$/.test(parts)) return parts + ":00Z";
    return parts.endsWith('Z') ? parts : parts + "Z";
  }
  // 0000-00-00* => treat as null
  if (isZeroDateIso(s) || s.startsWith('0000-00-00')) return null;
  return null;
}
function toNumberOrNull(v) {
  if (v === null || v === undefined) return null;
  const n = Number(String(v).replace(/[^\d\-,\.]/g, '').replace(',', '.'));
  return Number.isFinite(n) ? n : null;
}

/* grava amostra para debug */
function writeDebugSample(name, sample) {
  try {
    const base = 'C:\\temp';
    if (!fs.existsSync(base)) fs.mkdirSync(base, { recursive: true });
    const p = path.join(base, name);
    fs.writeFileSync(p, JSON.stringify(sample, null, 2), 'utf8');
    console.log(`DEBUG: amostra gravada em ${p}`);
  } catch (e) {
    console.error("DEBUG: falha ao gravar amostra", e);
  }
}

/* delete com fallback */
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
  }
}

/* upsert em lotes (usamos upsert para evitar duplicate key) */
async function upsertInBatches(table, rows, batch = 300, onConflict = null) {
  for (let i = 0; i < rows.length; i += batch) {
    const chunk = rows.slice(i, i + batch);
    if (chunk.length === 0) continue;
    let res;
    if (onConflict) {
      res = await supabase.from(table).upsert(chunk, { onConflict, returning: false });
    } else {
      res = await supabase.from(table).insert(chunk, { returning: false });
    }
    if (res.error) {
      console.error(`Error upserting into ${table} (offset ${i})`, res.error);
      throw res.error;
    }
    console.log(`Upserted ${chunk.length} into ${table} (offset ${i})`);
  }
}

/* ---------------- lógica de extração a partir do JSON no formato que você mostrou ---------------- */
function extractRowsFromGeneralJson(jsonArray) {
  const clientesRows = [];
  const pedidosRows = [];
  const produtosRows = [];

  const pedidosSamplesMissingCliente = [];
  const produtosSamplesMissingCliente = [];

  for (let idx = 0; idx < jsonArray.length; idx++) {
    const client = jsonArray[idx] || {};
    const clienteCodigo = safeString(client.codigo) || safeString(client.cliente_codigo) || null;

    // montar linha cliente
    const clienteRow = {
      cliente_codigo: clienteCodigo || null,
      codigo: clienteCodigo || null,
      nome: safeString(client.nome) || null,
      email: safeString(client.email) || null,
      data_cadastro: parseDateToIso(client.data_cadastro) || null,
      whatsapp: safeString(client.whatsapp) || null,
      cidade: safeString(client.cidade) || null,
      estado: safeString(client.estado) || null,
      loja_drop: safeString(client.loja_drop) || null,
      representante: safeString(client.representante) || null,
      total_pedidos: (client.total_pedidos != null) ? Number(client.total_pedidos) : null,
      valor_total_comprado: toNumberOrNull(client.valor_total_comprado),
      criado_em: new Date().toISOString()
    };
    clientesRows.push(clienteRow);

    // pedidos: cliente.pedidos (array)
    if (Array.isArray(client.pedidos)) {
      for (let p of client.pedidos) {
        const codigo_pedido = safeString(p.codigo_pedido) || safeString(p.id) || null;
        const pedidoRow = {
          codigo_pedido,
          cliente_codigo: clienteCodigo || '0', // preencher com cliente do bloco; se vazio -> '0'
          situacao_pedido: safeString(p.situacao_pedido) || null,
          data_hora_pedido: parseDateToIso(p.data_hora_pedido) || null,
          data_hora_confirmacao: parseDateToIso(p.data_hora_confirmacao) || null,
          data_hora_confirmacao_pagamento: parseDateToIso(p.data_hora_confirmacao_pagamento) || null,
          valor_total_produtos: toNumberOrNull(p.valor_total_produtos),
          valor_frete: toNumberOrNull(p.valor_frete),
          frete: safeString(p.frete) || null,
          valor_total_pedido: toNumberOrNull(p.valor_total_pedido),
          desconto: toNumberOrNull(p.desconto),
          cidade: safeString(p.cidade) || null,
          estado: safeString(p.estado) || null,
          percentual_comissao: toNumberOrNull(p.percentual_comissao),
          origem_pedido: safeString(p.origem_pedido) || null,
          tipo_compra: safeString(p.tipo_compra) || null,
          texto_tipo_compra: safeString(p.texto_tipo_compra) || null,
          pedidos_loja_drop: safeString(p.pedidos_loja_drop) || null,
          criado_em: new Date().toISOString()
        };
        // se clienteCodigo ausente, guarda amostra para debug
        if (!clienteCodigo) {
          if (pedidosSamplesMissingCliente.length < 20) pedidosSamplesMissingCliente.push({ clientIndex: idx, sample: pedidoRow });
        }
        pedidosRows.push(pedidoRow);
      }
    }

    // produtos_comprados: objeto com keys sendo o código (conforme exemplo)
    const produtosComprados = client.produtos_comprados;
    if (produtosComprados && typeof produtosComprados === 'object') {
      for (const key of Object.keys(produtosComprados)) {
        const p = produtosComprados[key] || {};
        const produto_codigo = safeString(p.codigo) || safeString(key) || null;
        const titulo = safeString(p.titulo) || null;
        const quantidade = (p.quantidade != null) ? (Number(String(p.quantidade).replace(/\D/g,'')) || Number(p.quantidade) || null) : null;
        const produtoRow = {
          cliente_codigo: clienteCodigo || '0',
          produto_codigo,
          titulo,
          categoria_principal: safeString(p.categoria_principal) || null,
          categoria: safeString(p.categoria) || null,
          marca: safeString(p.marca) || null,
          quantidade: quantidade,
          criado_em: new Date().toISOString()
        };
        if (!clienteCodigo) {
          if (produtosSamplesMissingCliente.length < 50) produtosSamplesMissingCliente.push({ clientIndex: idx, key, sample: produtoRow });
        }
        produtosRows.push(produtoRow);
      }
    }
  }

  // gravar amostras se houver problemas
  if (pedidosSamplesMissingCliente.length) writeDebugSample('bad_pedidos_missing_cliente.json', pedidosSamplesMissingCliente.slice(0, 50));
  if (produtosSamplesMissingCliente.length) writeDebugSample('bad_products_missing_cliente.json', produtosSamplesMissingCliente.slice(0, 50));

  return { clientesRows, pedidosRows, produtosRows };
}

/* ---------------- main ---------------- */
async function main() {
  const source = process.argv[2];
  if (!source || !fs.existsSync(source)) {
    console.error("Arquivo não encontrado:", source);
    process.exit(1);
  }

  const raw = fs.readFileSync(source, 'utf8');
  let json;
  try {
    json = JSON.parse(raw);
  } catch (e) {
    console.error("Erro parseando JSON:", e.message);
    process.exit(1);
  }

  // Se o JSON for um objeto com uma chave que contém o array principal, tentamos detectar
  let mainArray = null;
  if (Array.isArray(json)) mainArray = json;
  else {
    // procura a primeira propriedade que é array de objetos com 'codigo' (clientes)
    for (const k of Object.keys(json)) {
      if (Array.isArray(json[k])) {
        mainArray = json[k];
        break;
      }
    }
    if (!mainArray) {
      // fallback: tentar extrair de json.clients, json.lista_clientes, etc
      mainArray = json.clientes || json.lista_clientes || json.lista_clientes_geral || json.lista;
      if (!Array.isArray(mainArray)) {
        console.error("Não encontrei o array principal de clientes no JSON.");
        process.exit(1);
      }
    }
  }

  console.log(`→ ORIGINAIS:\nClientes: ${mainArray.length}`);
  // extrai linhas
  const { clientesRows, pedidosRows, produtosRows } = extractRowsFromGeneralJson(mainArray);

  console.log(`→ DEDUPE / contagens -> clientes: ${clientesRows.length}, pedidos: ${pedidosRows.length}, produtos: ${produtosRows.length}`);

  console.log("→ LIMPAR TABELAS...");
  await deleteAll('import_clientes');

  // upsert clientes (onConflict by 'codigo')
  try {
    await upsertInBatches('import_clientes', clientesRows, 300, 'codigo');
  } catch (e) {
    console.error("FATAL ao inserir import_clientes:", e);
    process.exit(1);
  }

  // garantir que exista placeholder '0' cliente caso haja pedidos/produtos com cliente '0'
  try {
    const zeros = clientesRows.find(r => r.codigo === '0' || r.cliente_codigo === '0');
    if (!zeros) {
      const placeholder = { codigo: '0', cliente_codigo: '0', nome: 'CLIENTE_DESCONHECIDO', criado_em: new Date().toISOString() };
      await supabase.from('import_clientes').upsert([placeholder], { onConflict: 'codigo', returning: false });
      console.log("Placeholders upserted 1 (cliente '0')");
    }
  } catch (e) {
    console.warn("Aviso: falha ao criar placeholder cliente '0' (pode já existir).", e.message || e);
  }

  // pedidos
  await deleteAll('import_pedidos');
  try {
    // antes de inserir, se houver pedidos com cliente_codigo nulo, já estarão '0' pelo extractor
    await upsertInBatches('import_pedidos', pedidosRows, 300, 'codigo_pedido');
  } catch (e) {
    console.error("FATAL ao inserir import_pedidos:", e);
    process.exit(1);
  }

  // produtos
  await deleteAll('import_clientes_produtos');
  try {
    // normaliza produto_codigo único e upsert por produto_codigo
    // se quiser upsert por (cliente_codigo, produto_codigo) altere onConflict adequadamente
    await upsertInBatches('import_clientes_produtos', produtosRows, 300, 'produto_codigo');
  } catch (e) {
    console.error("FATAL ao inserir import_clientes_produtos:", e);
    process.exit(1);
  }

  console.log("Sync finished successfully.");
}

main().catch(e => {
  console.error("Fatal error", e);
  process.exit(1);
});
