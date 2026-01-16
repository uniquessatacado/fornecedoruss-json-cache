const fs = require("fs");

// Carregar o JSON gigante
const raw = fs.readFileSync("./general.json", "utf8");
const clientes = JSON.parse(raw);

// Criar pastas se não existirem
if (!fs.existsSync("./clientes")) fs.mkdirSync("./clientes");
if (!fs.existsSync("./pedidos")) fs.mkdirSync("./pedidos");

const map = {}; // pedido → cliente

for (const cliente of clientes) {
  const codigoCliente = cliente.codigo;
  
  // Salvar arquivo do cliente
  const clientePath = `./clientes/${codigoCliente}.json`;
  fs.writeFileSync(clientePath, JSON.stringify(cliente, null, 2));
  
  // Mapear pedidos do cliente
  if (Array.isArray(cliente.pedidos)) {
    for (const pedido of cliente.pedidos) {
      map[pedido.codigo_pedido] = codigoCliente;
    }
  }
}

// Salvar índice reverso
fs.writeFileSync("./pedidos/map.json", JSON.stringify(map, null, 2));

console.log("Arquivos gerados com sucesso!");
