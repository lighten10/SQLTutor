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
  const planTreeDiv = document.getElementById("planTree");
  planDiv.innerHTML = 'Loading plan...';
  planTreeDiv.innerHTML = '';

  try {
    const result = await connection.query(`EXPLAIN ${sql}`);
    const planText = result.toArray().map(row => row.explain_value).join('\n');
    planDiv.innerHTML = `<pre>${planText}</pre>`;

    const treeData = parseExplainToTree(planText);
    renderD3Tree(treeData);
  } catch (err) {
    console.error("❌ Visualization Error:", err);
    planDiv.innerHTML = `<p class="text-red-600">❌ ${err.message}</p>`;
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
function renderPlanTree(parsedSteps) {
  const treeRoot = buildHierarchy(parsedSteps);

  // Clear existing tree
  const container = document.getElementById('planTree');
  container.innerHTML = '';

  const width = 600;
  const dx = 20;
  const dy = width / 3;
  const tree = d3.tree().nodeSize([dx, dy]);

  const diagonal = d3.linkHorizontal().x(d => d.y).y(d => d.x);

  const root = d3.hierarchy(treeRoot);
  root.x0 = dy / 2;
  root.y0 = 0;

  const svg = d3.create('svg')
    .attr('viewBox', [-dy / 3, -dx, width, dx * parsedSteps.length])
    .style('font', '12px sans-serif')
    .style('user-select', 'none');

  const gLink = svg.append('g')
    .attr('fill', 'none')
    .attr('stroke', '#888')
    .attr('stroke-opacity', 0.6)
    .attr('stroke-width', 1.5);

  const gNode = svg.append('g')
    .attr('cursor', 'pointer')
    .attr('pointer-events', 'all');

  function update(source) {
    const nodes = root.descendants();
    const links = root.links();

    tree(root);

    gNode.selectAll('g')
      .data(nodes)
      .join(enter => {
        const node = enter.append('g')
          .attr('transform', d => `translate(${d.y},${d.x})`);

        node.append('circle')
          .attr('r', 4)
          .attr('fill', d => d.children ? '#05CE78' : '#ccc');

        node.append('text')
          .attr('dy', '0.31em')
          .attr('x', d => d.children ? -8 : 8)
          .attr('text-anchor', d => d.children ? 'end' : 'start')
          .text(d => d.data.name)
          .clone(true).lower()
          .attr('stroke', 'white');

        return node;
      });

    gLink.selectAll('path')
      .data(links)
      .join('path')
      .attr('d', diagonal);
  }

  update(root);
  container.appendChild(svg.node());
}

function buildHierarchy(steps) {
  const root = { name: steps[0].content, children: [] };
  const stack = [{ node: root, level: steps[0].level }];

  for (let i = 1; i < steps.length; i++) {
    const step = steps[i];
    const node = { name: step.content, children: [] };

    while (stack.length && step.level <= stack[stack.length - 1].level) {
      stack.pop();
    }

    stack[stack.length - 1].node.children.push(node);
    stack.push({ node, level: step.level });
  }

  return root;
}



function parseExplainToTree(planText) {
  const lines = planText.trim().split('\n');
  const root = { name: "root", children: [] };
  const stack = [{ level: -1, node: root }];

  for (let line of lines) {
    const level = line.search(/\S/); // count indentation
    const name = line.trim().replace(/^─+/, '');
    const node = { name, children: [] };

    while (stack.length && level <= stack[stack.length - 1].level) {
      stack.pop();
    }

    stack[stack.length - 1].node.children.push(node);
    stack.push({ level, node });
  }

  return root.children[0]; // skip dummy root
}


function renderD3Tree(treeData) {
  const container = document.getElementById("planTree");
  container.innerHTML = ""; // Clear old tree

  if (!treeData || !treeData.name) {
    container.innerHTML = "<p class='text-sm text-red-400'>⚠️ Invalid plan data.</p>";
    return;
  }

  // 🛑 Handle single-node tree
  if (!treeData.children || treeData.children.length === 0) {
    container.innerHTML = `
      <div class="text-sm text-center text-gray-400 italic py-4">
        Only a single step in the plan (e.g., SEQ_SCAN) — nothing to visualize as a tree.
      </div>
    `;
    return;
  }

  // Setup
  const width = 600;
  const dx = 20;
  const dy = width / 4;

  const root = d3.hierarchy(treeData);
  root.x0 = dy / 2;
  root.y0 = 0;

  const treeLayout = d3.tree().nodeSize([dx, dy]);
  treeLayout(root);

  let x0 = Infinity, x1 = -Infinity;
  root.each(d => {
    if (d.x < x0) x0 = d.x;
    if (d.x > x1) x1 = d.x;
  });

  const height = x1 - x0 + dx * 2;

  const svg = d3.create("svg")
    .attr("viewBox", [0, 0, width, height])
    .attr("width", width)
    .attr("height", height);

  const g = svg.append("g")
    .attr("transform", `translate(${width / 2},${dx})`);

  // Links
  g.append("g")
    .selectAll("path")
    .data(root.links())
    .join("path")
    .attr("fill", "none")
    .attr("stroke", "#ccc")
    .attr("stroke-width", 1.5)
    .attr("d", d3.linkVertical()
      .x(d => d.x - width / 2)
      .y(d => d.y));

  // Nodes
  const node = g.append("g")
    .selectAll("g")
    .data(root.descendants())
    .join("g")
    .attr("transform", d => `translate(${d.x - width / 2},${d.y})`);

  node.append("circle")
    .attr("r", 6)
    .attr("fill", "#05CE78");

  node.append("text")
    .attr("dy", "-0.7em")
    .attr("text-anchor", "middle")
    .attr("font-size", "0.75rem")
    .attr("fill", "white")
    .text(d => d.data.name || "Node");

  // Mount to container
  container.appendChild(svg.node());
}




updateTableList();