const express = require('express');
const multer = require('multer');
const cors = require('cors');
const yaml = require('js-yaml');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const csvjson = require('csvtojson');

const app = express();
const upload = multer({ dest: 'uploads/' });
app.use(cors());

app.post('/run-etl', upload.single('inputFile'), async (req, res) => {
  try {
    const dataDir = path.join(__dirname, 'data');
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir);

    let uploadedPath = null;
    let fileType = null;
    if (req.file) {
      const ext = path.extname(req.file.originalname).toLowerCase();
      if (ext === '.csv') fileType = 'csv';
      else if (ext === '.json') fileType = 'json';
      else fileType = 'txt';
      uploadedPath = path.join(dataDir, 'uploaded_input' + ext);
      fs.renameSync(req.file.path, uploadedPath);
    }

    const configPath = path.join(__dirname, 'config.yaml');
    let config = yaml.load(fs.readFileSync(configPath, 'utf8'));
    if (uploadedPath) {
      config.extract.type = fileType;
      config.extract.source = 'data/uploaded_input' + path.extname(uploadedPath);

      config.extract.source_id = config.extract.source_id || 'default_source';
      fs.writeFileSync(configPath, yaml.dump(config));
    }

    let outputCsv = path.join(dataDir, 'output.csv');
    if (fs.existsSync(outputCsv)) fs.unlinkSync(outputCsv);

    const pyProcess = spawn('python3', ['run_etl.py'], { cwd: __dirname });
    let stdout = '', stderr = '';
    const timeout = setTimeout(() => {
      pyProcess.kill('SIGKILL');
      return res.json({ success: false, error: 'ETL timed out.' });
    }, 60000);
    pyProcess.stdout.on('data', data => { stdout += data; });
    pyProcess.stderr.on('data', data => { stderr += data; });

    pyProcess.on('close', async code => {
      clearTimeout(timeout);
      if (!fs.existsSync(outputCsv)) {
        return res.json({ success: false, error: 'No output produced.', details: stderr || stdout });
      }
      try {
        let tableJson = await csvjson().fromFile(outputCsv);

        let schemaId = config.extract.source_id || 'default_source';
        let schemaPath = path.join(__dirname, 'schemas', schemaId + '_schema.json');
        let schemaJson = null;
        if (fs.existsSync(schemaPath)) {
          schemaJson = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));
        }
        return res.json({ success: true, table: tableJson, schema: schemaJson });
      } catch (err) {
        return res.json({ success: false, error: err.message });
      }
    });
  } catch (err) {
    return res.json({ success: false, error: err.message });
  }
});

app.get('/schema/:sourceid', (req, res) => {
  const schemaPath = path.join(__dirname, 'schemas', req.params.sourceid + '_schema.json');
  if (!fs.existsSync(schemaPath)) {
    schemaJson = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));
    console.log("[DEBUG] Schema fetched and sent to frontend:", schemaJson);
    return res.status(404).json({ error: 'Schema not found.' });
  }
  const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf8'));
  res.json(schema);
});


app.get('/download', (req, res) => {
  const filePath = path.resolve(__dirname, 'data/output.csv');
  res.download(filePath, 'structured_table.csv'); 
});


app.listen(5001, () => {
  console.log('Backend running on http://localhost:5001');
});
