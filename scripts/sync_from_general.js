import fs from "fs";
import process from "process";
import path from "path";
import { fileURLToPath } from "url";
import { createClient } from "@supabase/supabase-js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE
);

// ----------- UTIL -----------
function iso(d) {
  if (!d) return null;

  d = String(d).trim();
  if (!d) return null;

  // zero-date em qualquer formato
  if (/^0{4}|^0000-|^0000\//.test(d)) return null;
  if (d === "0000-00-00T00:00:00Z") return null;

  // dd/mm/yyyy
  const m = d.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (m) {
    return `${m[3]}-${m[2]}-${m[1]}T00:00:00Z`;
  }

  // já parece ISO
  const isoLike = d.replace(" ", "T");
  if (/^\d{4}-\d{2}-\d{2}T/.test(isoLike)) {
    return isoLike.endsWith("Z") ? isoLike : `${isoLike}Z`;
  }

  return null;
}

function createdDate() {
  // coluna criado_em no Supabase é DATE (sem timezone)
  // então o formato correto é somente yyyy-mm-dd
  return new Date().toISOString().split("T")[0];
}

async function insertBatch(table, rows) {
  if (rows.length === 0) return;

  const { error } = await supabase.from(table).insert(rows);

  if (error) {
    console.error(`Batch error ${table}`, error);
    throw error;
  }
}

// ----------- MAIN -----------
async function run() {
  const source = process.argv[2];
  if (!source) {
    console.error("Missing JSON file path");
    process.exit(1);
  }

  const json = JSON.parse(fs.readFileSync(source, "utf8"));
  console.log(`→ Clientes no JSON: ${json.length}`);

  // limpar tabelas temp
  console.log("→ Limpando tabelas…");
  await supabase.from("import_clientes").delete().neq("codigo", -1);
  await supabase.from("import_pedidos").delete().neq("codigo_pedido", -1);
  await supabase.from("import_clientes_produtos").delete().neq("produto_codigo", "-1");

  // -------- CLIENTES --------
  const clientesBatch = [];
  const BATCH = 200;

  for (let offset = 0; offset < json.length; offset++) {
    const c = json[offset];

    const row = {
      codigo: c.codigo,
      nome: c.nome,
      email: c.email || null,
      sexo: c.sexo || null,
      whatsapp: c.whatsapp || null,
      cidade: c.cidade || null,
      estado: c.estado || null,
      data_cadastro: iso(c.data_cadastro),
      loja_drop: c.loja_drop ? c.loja_drop === "1" : false,
      representante: c.representante ? c.representante === "1" : false,
      total_pedidos: c.total_pedidos || 0,
      valor_total_comprado: c.valor_total_comprado || 0,
      criado_em: createdDate(),
    };

    clientesBatch.push(row);

    if (clientesBatch.length === BATCH) {
      await insertBatch("import_clientes", clientesBatch);
      console.log(`→ import_clientes: inserted ${BATCH} (offset ${offset})`);
      clientesBatch.length = 0;
    }
  }

  if (clientesBatch.length) {
    await insertBatch("import_clientes", clientesBatch);
    console.log("→ import_clientes: final batch inserted");
  }

  // -------- PEDIDOS --------
  const pedidosBatch = [];

  for (let c of json) {
    const clienteCodigo = c.codigo;

    if (!Array.isArray(c.pedidos)) continue;

    for (let p of c.pedidos) {
      pedidosBatch.push({
        cliente_codigo: clienteCodigo,
        codigo_pedido: p.codigo_pedido,
        situacao_pedido: p.situacao_pedido || null,
        data_hora_pedido: iso(p.data_hora_pedido),
        data_hora_confirmacao: iso(p.data_hora_confirmacao),
        data_hora_confirmacao_pagamento: iso(p.data_hora_confirmacao_pagamento),
        valor_total_produtos: p.valor_total_produtos || 0,
        valor_frete: p.valor_frete || 0,
        frete: p.frete || null,
        valor_total_pedido: p.valor_total_pedido || 0,
        desconto: p.desconto || 0,
        cidade: p.cidade || c.cidade || null,
        estado: p.estado || c.estado || null,
        percentual_comissao: p.percentual_comissao || 0,
        origem_pedido: p.origem_pedido || null,
        tipo_compra: p.tipo_compra || null,
        texto_tipo_compra: p.texto_tipo_compra || null,
        pedidos_loja_drop: p.pedidos_loja_drop === "SIM",
        criado_em: createdDate(),
      });

      if (pedidosBatch.length >= BATCH) {
        await insertBatch("import_pedidos", pedidosBatch);
        console.log("Inserted 300 into import_pedidos");
        pedidosBatch.length = 0;
      }
    }
  }

  if (pedidosBatch.length) {
    await insertBatch("import_pedidos", pedidosBatch);
    console.log("Inserted final pedidos batch");
  }

  // -------- PRODUTOS --------
  const produtosBatch = [];

  for (let c of json) {
    const clienteCodigo = c.codigo;

    const produtos = c.produtos_comprados;
    if (!produtos) continue;

    for (let key of Object.keys(produtos)) {
      const p = produtos[key];

      produtosBatch.push({
        cliente_codigo: clienteCodigo,
        produto_codigo: p.codigo,
        titulo: p.titulo || null,
        categoria_principal: p.categoria_principal || null,
        categoria: p.categoria || null,
        marca: p.marca || null,
        quantidade: Number(p.quantidade || 0),
        criado_em: createdDate(),
      });

      if (produtosBatch.length >= BATCH) {
        await insertBatch("import_clientes_produtos", produtosBatch);
        console.log("Inserted 300 into import_clientes_produtos");
        produtosBatch.length = 0;
      }
    }
  }

  if (produtosBatch.length) {
    await insertBatch("import_clientes_produtos", produtosBatch);
    console.log("Inserted final produtos batch");
  }

  console.log("✔ Sync finalizado sem erros.");
}

run().catch((e) => {
  console.error("Fatal error", e);
  process.exit(1);
});
