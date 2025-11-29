const fs = require('fs');
const path = require('path');
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

function moneyToNumber(str) {
  if (!str) return null;
  const cleaned = String(str)
    .replace(/[^\d,.-]/g, "")
    .replace(/\./g, "")
    .replace(/,/g, ".");
  const n = parseFloat(cleaned);
  return isNaN(n) ? null : n;
}

function normalizeCarrinho(item) {
  return {
    carrinho_id: String(item.id ?? item.carrinho_id ?? ''),
    id_clientes: item.id_clientes ?? null,
    nome: item.nome ?? item.name ?? null,
    email: item.email ?? null,
    celular: item.celular ?? item.telefone ?? null,
    data_transacao: item.data_transacao ?? null,
    hora_transacao: item.hora_transacao ?? null,
    total_valor: moneyToNumber(item.valor_total ?? item["valor total"] ?? null),
    produtos: item.produtos ?? [],
    raw: item
  };
}

async function upsertCarrinhos(carrinhos) {
  const batchSize = 200;

  for (let i = 0; i < carrinhos.length; i += batchSize) {
    const batch = carrinhos.slice(i, i + batchSize);

    const { error } = await supabase
      .from('carrinhos')
      .upsert(batch, { onConflict: 'carrinho_id' });

    if (error) {
      console.error("Erro ao fazer upsert:", error);
      throw error;
    } else {
      console.log(`Upsert carrinhos OK (offset ${i})`);
    }
  }
}

async function deleteMissingCarrinhos(currentIds) {
  const { data: rows, error } = await supabase
    .from('carrinhos')
    .select('carrinho_id');

  if (error) return console.error("Erro lendo carrinhos:", error);

  const toDelete = rows
    .filter(row => !currentIds.includes(String(row.carrinho_id)))
    .map(r => r.carrinho_id);

  if (toDelete.length === 0) {
    console.log("Nenhum carrinho para remover.");
    return;
  }

  console.log("Removendo carrinhos inexistentes no JSON:", toDelete.length);

  await supabase.from('carrinho_produtos').delete().in('carrinho_id_text', toDelete);
  await supabase.from('carrinhos').delete().in('carrinho_id', toDelete);

  console.log("Carrinhos removidos.");
}

async function syncProducts(carrinhos) {
  let allProducts = [];

  for (const c of carrinhos) {
    const cid = String(c.carrinho_id ?? c.id ?? '');

    await supabase
      .from('carrinho_produtos')
      .delete()
      .eq('carrinho_id_text', cid);

    if (!Array.isArray(c.produtos)) continue;

    c.produtos.forEach(p => {
      allProducts.push({
        carrinho_id_text: cid,
        produto_codigo: p.codigo ?? null,
        nome_produto: p.produto ?? null,
        imagem: p.imagem ?? null,
        tamanho: p.tamanho ?? null,
        cor: p.cor ?? null,
        categoria: p.categoria ?? null,
        categoria_principal: p.categoria_principal ?? null,
        marca: p.marca ?? null,
        valor_unitario: moneyToNumber(p.valor_unitario),
        quantidade: parseInt(p.quantidade ?? 1),
        valor_total: moneyToNumber(p.valor_total),
        raw: p
      });
    });
  }

  console.log(`Inserindo ${allProducts.length} produtos...`);

  const batchSize = 200;

  for (let i = 0; i < allProducts.length; i += batchSize) {
    const batch = allProducts.slice(i, i + batchSize);

    const { error } = await supabase
      .from('carrinho_produtos')
      .insert(batch, { returning: false });

    if (error) {
      console.error("Erro inserindo produtos:", error);
      throw error;
    }

    console.log(`Batch produtos OK (offset ${i})`);
  }
}

async function main() {
  const source = process.argv[2];

  if (!fs.existsSync(source)) {
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

  const items = json.lista_carrinhos ?? json.carrinhos ?? json ?? [];

  const normalized = items.map(normalizeCarrinho);

  console.log("Carrinhos encontrados:", normalized.length);

  await upsertCarrinhos(normalized);

  const ids = normalized.map(i => String(i.carrinho_id));
  await deleteMissingCarrinhos(ids);

  await syncProducts(normalized);

  console.log("Sincronização completa!");
}

main().catch(err => {
  console.error("Erro fatal:", err);
  process.exit(1);
});
