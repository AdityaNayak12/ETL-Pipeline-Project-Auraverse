// table.js

document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('etlForm');
  const input = document.getElementById('uploadInput');
  const loading = document.getElementById('loading');
  const output = document.getElementById('output');
  const fileNameLabel = document.getElementById('fileName');

  // New: Schema container
  const schemaContainer = document.getElementById('schemaContainer');

  // Show filename when selected
  input.addEventListener('change', () => {
    fileNameLabel.textContent = input.files.length
      ? `📄 ${input.files[0].name}`
      : "No file selected";
  });

  form.addEventListener('submit', ev => {
    ev.preventDefault();
    output.innerHTML = '';
    if(schemaContainer) schemaContainer.innerHTML = '';
    loading.style.display = 'block';

    let fd = new FormData();
    if (input.files.length) {
      fd.append('inputFile', input.files[0]);
    }

    fetch('http://localhost:5001/run-etl', {
      method: 'POST',
      body: fd,
    })
      .then(res => res.json())
      .then(data => {
        console.log('ETL response:', data);
        loading.style.display = 'none';

        if (data.success && data.table && data.table.length) {
          renderTable(data.table);
          renderSchema(data.schema || null);
        } else {
          output.innerHTML = `<div style="color:#d02927;font-weight:bold;">
            ❌ Error: ${data.error || 'No data returned.'}</div>`;
          renderSchema(null);
        }
        
      })
      .catch((err) => {
        loading.style.display = 'none';
        console.error(err);
        output.innerHTML = `<div style="color:#d02927;font-weight:bold;">
        🚨 Network or server error. Ensure backend is running.</div>`;
        renderSchema(null);
      });
      
  });

  function renderTable(rows) {
    let html = '<table><thead><tr>';
    html += Object.keys(rows[0]).map(key => `<th>${key}</th>`).join('');
    html += '</tr></thead><tbody>';

    rows.forEach(row => {
      html += `<tr>${Object.values(row)
        .map(val => `<td>${val ?? ''}</td>`).join('')}</tr>`;
    });

    html += '</tbody></table>';
    output.innerHTML = html;
  }

  // New: Pretty-print schema JSON below the table
  function renderSchema(schema) {
    if (!schemaContainer) return;
    if (!schema) {
      schemaContainer.innerHTML = '';
      return;
    }
    schemaContainer.innerHTML = `
      <h2>Detected Schema</h2>
      <pre style="background:#f8f8fc;padding:18px;border-radius:8px;margin-top:10px;text-align:left;max-height:500px;overflow:auto;font-size:0.95rem;">
${JSON.stringify(schema, null, 2)}
      </pre>
    `;
  }
});
