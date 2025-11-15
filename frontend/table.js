document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('etlForm');
  const input = document.getElementById('uploadInput');
  const loading = document.getElementById('loading');
  const output = document.getElementById('output');
  const fileNameLabel = document.getElementById('fileName');
  const downloadBtn = document.getElementById('downloadBtn');

  // Show filename when selected
  input.addEventListener('change', () => {
    fileNameLabel.textContent = input.files.length
      ? `📄 ${input.files[0].name}`
      : "No file selected";
  });

  form.addEventListener('submit', ev => {
    ev.preventDefault();
    output.innerHTML = '';
    loading.style.display = 'block';

    let fd = new FormData();
    if (input.files.length) {
      fd.append('inputFile', input.files[0]);
    }

    fetch('http://127.0.0.1:5001/run-etl', {
      method: 'POST',
      body: fd,
    })
      .then(res => res.json())
      .then(data => {
        loading.style.display = 'none';

        if (data.success && data.table && data.table.length) {
          renderTable(data.table);
        } else {
          output.innerHTML = `<div style="color:#d02927;font-weight:bold;">
            ❌ Error: ${data.error || 'No data returned.'}</div>`;
        }
      })
      .catch((err) => {
        loading.style.display = 'none';
        console.error(err);
        output.innerHTML = `<div style="color:#d02927;font-weight:bold;">
        🚨 Network or server error. Ensure backend is running.</div>`;
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

  // Download button will just hit backend, no change required
});
