const fs = require("fs");
const path = require("path");

// Pasta onde estão os JSON dos clientes
const clientesDir = path.join(__dirname, "..", "clientes");

const lookup = {};

// Função para limpar o número
const normalize = (wpp) => {
  if (!wpp) return null;
  return wpp.replace(/\D+/g, ""); // Remove tudo que não é número
};

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

// Salvar o arquivo final
fs.writeFileSync(
  path.join(__dirname, "..", "lookup-whatsapp.json"),
  JSON.stringify(lookup, null, 2),
  "utf8"
);

console.log("Arquivo lookup-whatsapp.json gerado com sucesso!");
