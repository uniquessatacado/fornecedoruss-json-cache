const fs = require("fs");
const path = require("path");

// Pasta dos JSON de clientes
const clientesDir = path.join(__dirname, "..", "clientes");

// Objeto que vai conter: whatsapp → codigo_cliente
const lookup = {};

// Função para limpar o número (remove qualquer caractere que não seja dígito)
function normalize(wpp) {
  if (!wpp) return null;
  return wpp.replace(/\D+/g, ""); // \D = tudo que não é número
}

// Lê todos os arquivos da pasta clientes
fs.readdirSync(clientesDir).forEach((file) => {
  if (!file.endsWith(".json")) return;

  const filePath = path.join(clientesDir, file);
  const data = JSON.parse(fs.readFileSync(filePath, "utf8"));

  const wpp = normalize(data.whatsapp);
  const codigo = data.codigo;

  if (wpp && codigo) {
    lookup[wpp] = codigo;
  }
});

// Garante que a pasta /lookup existe
const lookupDir = path.join(__dirname, "..", "lookup");
if (!fs.existsSync(lookupDir)) {
  fs.mkdirSync(lookupDir);
}

// Caminho final do arquivo
const outputPath = path.join(lookupDir, "lookup_whatsapp.json");

// Salvar o lookup
fs.writeFileSync(outputPath, JSON.stringify(lookup, null, 2), "utf8");

console.log("Arquivo lookup_whatsapp.json gerado com sucesso!");
