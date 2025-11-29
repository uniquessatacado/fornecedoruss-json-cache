/* scripts/sync_from_general.js
   Versão FINAL — 100% compatível com o JSON real do FornecedorUSS (2025-11-29)
   Estrutura suportada:
   [
     {
       codigo: "123",
       nome: "...",
       pedidos: [...],
       produtos_comprados: {...}
     }
   ]
*/

const fs = require('fs');
const { createClient } = require('@supabase/supabase-js');

/* ------------------------ CONFIG SUPABASE ------------------------ */

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, {
  auth: { persistSession: false }
});

/* ------------------------ HELPERS ------------------------ */

function clean(v) {
  if (v === undefined || v === null) return null;
  return String(v).trim();
}

function normalizeCodigo(v) {
  if (!v) return null;
  return String(v).trim().replace(/[^\w\-\._]/g, '');
}

function toNumber(v) {
  if (v === undefined || v === null) return null;
  const n = Number(String(v).replace(',', '.'));
  return isNaN(n) ? null : n;
}

function toIsoDate(v) {
  if (!v) return null;
  const s = String(v).trim();

  // dd/mm/yyyy
  const m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})(?: (\d{2}:\d{2}:\d{2}))?$/);
  if (m) {
    const day = m[1];
    const mon = m[2];
    const year = m[3];
    const time = m[4] || "00:00:00";
    return `${year}-${mon}-${day}T${time}Z`;
  }

  // 0000-00-00
  if (s.startsWith("0000-00-00")) return null;

  // 2024-10-05 13:22:11
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) {
    return s.replace(' ', 'T') + (s.includes('Z') ? '' : 'Z');
  }

  return null;
}

async function deleteAll(table) {
  console.log(`→ Limpando tabela ${table}...`);
  const { error } = await supabase.from(table).delete().not("id", "is", null);
  if (error) console.error(`Erro limpando ${table}:`, error);
}

async function batchInsert(table, rows, batchSize = 200) {
  for (let i = 0; i < rows.length; i += batchSize) {
    const chunk = rows.slice(i, i + batchSize);
    console.log(`→ Inserindo ${chunk.length} em ${table} (offset ${i})`);
    const { error } = await supabase.from(table).insert(chunk, { returning: false });

    if (error) {
      console.error(`Erro em ${table} offset ${i}`, error);

      // fallback por linha
      for (let r = 0; r < chunk.length; r++) {
        const { error: e2 } = await supabase
          .from(table)
          .insert([chunk[r]], { returning: false });

        if (e2) {
          console.error("ROW ERROR:", e2);
        }
      }
    }
  }
}

/* ------------------------ MAIN ------------------------ */

async function main() {
  const file = process.argv[2];

  if (!file || !fs.existsSync(file)) {
    console.error("Arquivo JSON não encontrado:", file);
    process.exit(1);
  }

  const raw = fs.readFileSync(file, "utf8");
  let data;

  try {
    data = JSON.parse(raw);
  } catch (e) {
    console.error("Erro parseando JSON:", e);
    process.exit(1);
  }

  if (!Array.isArray(data)) {
    console.error("JSON raiz precisa ser um array de clientes.");
    process.exit(1);
  }

  const clientesRows = [];
  const pedidosRows = [];
  const produtosRows = [];

  /* ------------------------ EXTRAÇÃO REAL ------------------------ */

  for (const cli of data) {

    const clienteCodigo = normalizeCodigo(cli.codigo) || normalizeCodigo(cli.cliente_codigo);

    /* --- CLIENTE --- */
    clientesRows.push({
      cliente_codigo: clienteCodigo,
      codigo: clienteCodigo,
      nome: clean(cli.nome),
      email: clean(cli.email),
      data_cadastro: toIsoDate(cli.data_cadastro),
      whatsapp: clean(cli.whatsapp),
      cidade: clean(cli.cidade),
      estado: clean(cli.estado),
      loja_drop: clean(cli.loja_drop),
      representante: clean(cli.representante),
      total_pedidos: toNumber(cli.total_pedidos),
      valor_total_comprado: toNumber(cli.valor_total_comprado),
      criado_em: new Date().toISOString()
    });

    /* --- PEDIDOS --- */
    if (Array.isArray(cli.pedidos)) {
      for (const p of cli.pedidos) {
        pedidosRows.push({
          codigo_pedido: normalizeCodigo(p.codigo_pedido) || normalizeCodigo(p.id),
          cliente_codigo: clienteCodigo || '0',
          situacao_pedido: clean(p.situacao_pedido),
          data_hora_pedido: toIsoDate(p.data_hora_pedido),
          data_hora_confirmacao: toIsoDate(p.data_hora_confirmacao),
          data_hora_confirmacao_pagamento: toIsoDate(p.data_hora_confirmacao_pagamento),
          valor_total_produtos: toNumber(p.valor_total_produtos),
          valor_frete: toNumber(p.valor_frete),
          frete: clean(p.frete),
          valor_total_pedido: toNumber(p.valor_total_pedido),
          desconto: toNumber(p.desconto),
          cidade: clean(p.cidade),
          estado: clean(p.estado),
          percentual_comissao: toNumber(p.percentual_comissao),
          origem_pedido: clean(p.origem_pedido),
          tipo_compra: clean(p.tipo_compra),
          texto_tipo_compra: clean(p.texto_tipo_compra),
          pedidos_loja_drop: clean(p.pedidos_loja_drop),
          criado_em: new Date().toISOString()
        });
      }
    }

    /* --- PRODUTOS --- */
    if (cli.produtos_comprados && typeof cli.produtos_comprados === "object") {
      for (const key of Object.keys(cli.produtos_comprados)) {
        const pr = cli.produtos_comprados[key];

        produtosRows.push({
          cliente_codigo: clienteCodigo || '0',
          produto_codigo: normalizeCodigo(pr.codigo) || normalizeCodigo(key),
          titulo: clean(pr.titulo),
          categoria_principal: clean(pr.categoria_principal),
          categoria: clean(pr.categoria),
          marca: clean(pr.marca),
          quantidade: toNumber(pr.quantidade),
          subcategoria: clean(pr.subcategoria),
          tamanho: clean(pr.tamanho),
          cor: clean(pr.cor),
          sku: clean(pr.sku),
          valor_unitario: null,
          criado_em: new Date().toISOString()
        });
      }
    }
  }

  console.log(`Clientes: ${clientesRows.length}`);
  console.log(`Pedidos: ${pedidosRows.length}`);
  console.log(`Produtos: ${produtosRows.length}`);

  /* ------------------------ RESET E INSERT ------------------------ */

  await deleteAll("import_clientes");
  await deleteAll("import_pedidos");
  await deleteAll("import_clientes_produtos");

  await batchInsert("import_clientes", clientesRows, 200);
  await batchInsert("import_pedidos", pedidosRows, 200);
  await batchInsert("import_clientes_produtos", produtosRows, 200);

  console.log("✔ SYNC FINALIZADO COM SUCESSO ✔");
}

/* ------------------------ START ------------------------ */
main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
