const fs = require('fs');
const path = require('path');
const axios = require('axios');

async function main() {
  console.log("Baixando JSON do fornecedor...");
  const { data: clientes } = await axios.get("https://admin.fornecedoruss.com.br/processos/json/lista-dados-gerais.json");

  const pedidosDir = path.join(process.cwd(), "pedidos");
  const lookupDir = path.join(process.cwd(), "lookup");
  const lookupPath = path.join(lookupDir, "pedidos.json");

  if (!fs.existsSync(pedidosDir)) {
    fs.mkdirSync(pedidosDir, { recursive: true });
  }
  if (!fs.existsSync(lookupDir)) {
    fs.mkdirSync(lookupDir, { recursive: true });
  }

  const lookup = {};

  console.log("Processando pedidos...");
  for (const cliente of clientes) {
    const cliId = cliente.codigo;
    if (!cliente.pedidos) continue;

    for (const pedido of cliente.pedidos) {
      const cod = pedido.codigo_pedido;
      if (!cod) continue;

      lookup[cod] = cliId;

      const pedidoFile = path.join(pedidosDir, `${cod}.json`);
      const pedidoObj = {
        pedido: cod,
        cliente_id: cliId,
        ...pedido
      };

      fs.writeFileSync(pedidoFile, JSON.stringify(pedidoObj, null, 2));
    }
  }

  fs.writeFileSync(lookupPath, JSON.stringify(lookup, null, 2));
  console.log("Feito! Pedidos extraídos e lookup criado.");
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
