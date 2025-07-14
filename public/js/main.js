import * as duckdb from '@duckdb/duckdb-wasm';
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import mvp_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';

let db, connection;

// Initialize DuckDB
async function initDuckDB() {
  const logger = new duckdb.ConsoleLogger();
  const worker = new Worker(mvp_worker);
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(duckdb_wasm);
  connection = await db.connect();
  console.log('✅ DuckDB connected');
}

await initDuckDB();

// 🧩 Upload CSV
window.uploadCSV = async function () {
  const fileInput = document.getElementById("csvInput");
  const file = fileInput.files[0];
  if (!file) {
    alert("Please select a CSV file.");
    return;
  }

  const tableName = prompt("Enter a table name for this CSV:", file.name.split(".")[0]);
  if (!tableName) {
    alert("❌ Table name is required.");
    return;
  }

  try {
    const buffer = await file.arrayBuffer();
    const uint8Array = new Uint8Array(buffer);
    const filePath = `/${tableName}.csv`;

    // ✅ Register file with correct instance
    await db.registerFileBuffer(filePath, uint8Array);

    // ✅ Read into DuckDB
    await connection.query(`CREATE TABLE ${tableName} AS SELECT * FROM read_csv_auto('${filePath}')`);

    alert(`✅ Table '${tableName}' created successfully.`);
    updateTableList();  // Optional: shows all tables in a <div>
  } catch (error) {
    console.error("CSV Upload Error:", error);
    alert("❌ Failed to upload CSV.");
  }
};



async function updateTableList() {
  const result = await connection.query("SHOW TABLES;");
  const tableDiv = document.getElementById("tables");
  tableDiv.innerHTML = "";

  const tableList = document.createElement("ul");
  for (const row of result) {
    const li = document.createElement("li");
    li.textContent = row.name;
    tableList.appendChild(li);
  }
  tableDiv.appendChild(tableList);
}



// 🧮 Run Query
window.runQuery = async function () {
  const sql = document.getElementById("sql").value.trim();
  const resultDiv = document.getElementById("result");
  resultDiv.innerHTML = "";

  try {
    if (!connection) {
      resultDiv.innerHTML = `<p class="text-red-600 font-medium">❌ DuckDB not connected. Please wait...</p>`;
      return;
    }

    if (!sql) {
      resultDiv.innerHTML = `<p class="text-red-600 font-medium">❌ Please enter a SQL query.</p>`;
      return;
    }

    const result = await connection.query(sql);
    const rows = result.toArray();

    if (rows.length === 0) {
      resultDiv.innerHTML = `<p class="text-green-600 font-medium">✅ Query executed successfully. No rows returned.</p>`;
      return;
    }

    const headers = Object.keys(rows[0]);

    // Generate HTML Table
    const html = `
      <div class="overflow-x-auto">
        <table class="min-w-full table-auto border-collapse border border-gray-300 text-sm font-mono rounded-md">
          <thead class="bg-gray-100">
            <tr>
              ${headers.map(col => `<th class="border border-gray-300 px-4 py-2 text-left font-semibold">${col}</th>`).join("")}
            </tr>
          </thead>
          <tbody>
            ${rows.map(row => `
              <tr class="hover:bg-gray-50">
                ${headers.map(col => `<td class="border border-gray-200 px-4 py-2">${row[col]}</td>`).join("")}
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    `;

    resultDiv.innerHTML = html;

  } catch (err) {
    console.error("❌ Query Error:", err);
    resultDiv.innerHTML = `<p class="text-red-600 font-medium">❌ Query failed: ${err.message}</p>`;
  }
};


// 🔍 Visualize EXPLAIN Query
window.visualizeQuery = async function () {
  const sql = document.getElementById("sql").value;
  const planDiv = document.getElementById("plan");
  planDiv.innerHTML = '<p>Loading Plan...</p>';

  try {
    const explainSQL = `EXPLAIN ${sql}`;
    const result = await connection.query(explainSQL);
    const rows = result.toArray();

    const planText = rows.map(row => row["explain_value"]).join('\n');
    planDiv.innerHTML = `<pre>${planText}</pre>`;

    // Optional: Render tree-style plan
    const parsed = parseExplainPlan(planText);
    renderPlanTree(parsed);
  } catch (err) {
    console.error("❌ Visualization Error:", err);
    planDiv.innerHTML = `<p style="color:red;">❌ Visualization failed: ${err.message}</p>`;
  }
};

// 🧠 Plan Parser
function parseExplainPlan(planText) {
  const lines = planText.split('\n');
  return lines.map((line, index) => {
    const level = line.search(/\S/);
    return { level, content: line.trim(), index };
  });
}

// 🧠 Plan Tree Renderer
function renderPlanTree(steps) {
  const planDiv = document.getElementById("plan");
  const ul = document.createElement("ul");

  for (const step of steps) {
    const li = document.createElement("li");
    li.textContent = step.content;
    li.style.marginLeft = `${step.level * 10}px`;
    ul.appendChild(li);
  }

  planDiv.appendChild(ul);
}

updateTableList();