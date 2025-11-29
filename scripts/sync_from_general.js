/*  scripts/sync_from_general.js
    Versão FINAL — compatível com o JSON real do cliente (2025-11-29)
*/

const fs = require('fs');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing Supabase credentials.");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: false } });

/* ---------------------- helpers ---------------------- */

function safe(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function num(v) {
  if (v === undefined || v === null) return null;
  const s = String(v).replace(/[^\d\-.,]/g, '').replace(',', '.');
  const n = parseFloat(s);
  return isNaN(n) ? null : n;
}

function iso(d) {
  if (!d) return null;
  d = String(d).trim();
  if (d === "") return null;
  if (/^0000/.test(d)) return null;

  // formato dd/mm/yyyy
  const m = d.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (m) return `${m[3]}-${m[2]}-${m[1]}T00:00:00Z`;

  // formato "2024-03-18 10:12:40"
  const isoLike = d.replace(' ', 'T');
  if (/^\d{4}-\d{2}-\d{2}T/.test(isoLike)) return isoLike + "Z";

  return null;
}

/* ------------------------------------------------------ */

async function clear(table) {
  try {
    await supabase.from(table).delete().gt('id', 0);
  } catch (e) {
    console.error(`Erro limpando ${table}`, e.message);
  }
}

async function insertBatch(table, rows, batch, conflictColumn) {
  for (let i = 0; i < rows.length; i += batch) {
    const chunk = rows.slice(i, i + batch);
    const { error } = await supabase
      .from(table)
      .upsert(chunk, { onConflict: conflictColumn, returning: false });

    if (error) {
      console.error("Batch error", table, error);
      throw error;
    }

    console.log(`→ ${table}: inserted ${chunk.length} (offset ${i})`);
  }
}

/* ------------------------------------------------------ */

async function main() {
  const file = process.argv[2];
  if (!file || !fs.existsSync(file)) {
    console.error("Arquivo não encontrado");
    process.exit(1);
  }

  const raw = fs.readFileSync(file, 'utf8');
  let json;
  try {
    json = JSON.parse(raw);
  } catch (e) {
    console.error("Erro no JSON", e.message);
    process.exit(1);
  }

  if (!Array.isArray(json)) {
    console.error("JSON principal não é array.");
    process.exit(1);
  }

  console.log("→ Clientes no JSON:", json.length);

  const clientesRows = [];
  const pedidosRows = [];
  const produtosRows = [];

  for (const client of json) {
    const cliente_codigo = safe(client.codigo) || safe(client.cliente_codigo);

    /* ----- CLIENTE ----- */
    clientesRows.push({
      cliente_codigo,
      codigo: cliente_codigo,
      nome: safe(client.nome),
      email: safe(client.email),
      data_cadastro: iso(client.data_cadastro),
      whatsapp: safe(client.whatsapp),
      cidade: safe(client.cidade),
      estado: safe(client.estado),
      loja_drop: safe(client.loja_drop),
      representante: safe(client.representante),
      total_pedidos: num(client.total_pedidos),
      valor_total_comprado: num(client.valor_total_comprado),
      criado_em: new Date().toISOString()
    });

    /* ----- PEDIDOS ----- */
    if (Array.isArray(client.pedidos)) {
      for (const p of client.pedidos) {
        pedidosRows.push({
          codigo_pedido: safe(p.codigo_pedido),
          cliente_codigo,
          situacao_pedido: safe(p.situacao_pedido),
          data_hora_pedido: iso(p.data_hora_pedido),
          data_hora_confirmacao: iso(p.data_hora_confirmacao),
          data_hora_confirmacao_pagamento: iso(p.data_hora_confirmacao_pagamento),
          valor_total_produtos: num(p.valor_total_produtos),
          valor_frete: num(p.valor_frete),
          frete: safe(p.frete),
          valor_total_pedido: num(p.valor_total_pedido),
          desconto: num(p.desconto),
          cidade: safe(p.cidade),
          estado: safe(p.estado),
          percentual_comissao: num(p.percentual_comissao),
          origem_pedido: safe(p.origem_pedido),
          tipo_compra: safe(p.tipo_compra),
          texto_tipo_compra: safe(p.texto_tipo_compra),
          pedidos_loja_drop: safe(p.pedidos_loja_drop),
          criado_em: new Date().toISOString()
        });
      }
    }

    /* ----- PRODUTOS COMPRADOS ----- */
    if (client.produtos_comprados && typeof client.produtos_comprados === "object") {
      for (const key of Object.keys(client.produtos_comprados)) {
        const pr = client.produtos_comprados[key];

        produtosRows.push({
          cliente_codigo,
          produto_codigo: safe(pr.codigo) || safe(key),
          titulo: safe(pr.titulo),
          categoria_principal: safe(pr.categoria_principal),
          categoria: safe(pr.categoria),
          marca: safe(pr.marca),
          quantidade: num(pr.quantidade),
          criado_em: new Date().toISOString()
        });
      }
    }
  }

  /* ---- LIMPAR E INSERIR ---- */

  console.log("→ Limpando tabelas…");
  await clear("import_clientes");
  await clear("import_pedidos");
  await clear("import_clientes_produtos");

  await insertBatch("import_clientes", clientesRows, 200, "codigo");
  await insertBatch("import_pedidos", pedidosRows, 200, "codigo_pedido");
  await insertBatch("import_clientes_produtos", produtosRows, 300, "produto_codigo");

  console.log("→ FINALIZADO COM SUCESSO!");
}

main();
