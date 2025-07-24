# 🚀 csv-to-flatbuffer

High-performance tool to convert CSV/TXT financial tick/OHLCV data into **FlatBuffer binary format** for ultra-fast backtesting.

Supports:
- ✅ Zero-copy reading via `mmap`
- ✅ Multi-threaded conversion
- ✅ Resampling to 1/2/3/4/5min, 1d
- ✅ Fast random access via `.idx` index
- ✅ Human-readable output with timestamp formatting

Ideal for building **event-driven backtesters** and **low-latency trading systems**.

---

## 📦 Features

| Feature | Description |
|--------|-----------|
| ⚡ Speed | Uses `mmap`, `rayon`, and FlatBuffers for zero-copy processing |
| 🔍 Indexing | Generates `.idx` file with per-second and per-day access |
| 🧮 Resampling | Convert 1-minute data to 5min/daily without loading all data |
| 📁 Batch Support | Processes entire directories of CSV files |
| 🖥️ Cross-platform | Works on Linux, macOS, Windows |

---

## 🛠 Installation

```bash
git clone https://github.com/yourname/csv-to-flatbuffer.git
cd csv-to-flatbuffer
cargo build --release
```

---

## Dependencies:

Rust 1.78+
flatc compiler (optional, build script handles it)

---

## ▶️ Usage

### Convert CSV to FlatBuffer
```bash
cargo run --release -- \
  -i /path/to/csv/dir \
  -o /path/to/output.bin \
  -t 8
```

### Convert + Read + Resample
```bash
cargo run --release -- \
  -i /path/to/csv/dir \
  -o /path/to/output.bin \
  -t 8 \
  -c \
  -r 5min
```

---

## 🔤 Arguments

| Flag | Description |
|--------|-----------|
| -i, --input | Input directory with CSV/TXT files |
| -o, --output | Output .bin file path |
| -t, --threads | Number of threads (default: all cores) |
| -c, --check | After conversion, read and print first 5 bars |
| -r, --resample | Resample to: 1min, 2min, 3min, 4min, 5min, 1d (requires -c) |

💡 Example: -r 5min aggregates 1-minute bars into 5-minute candles. 

---

## 📄 Input CSV Format

The tool expects CSV files with the following header and format :

DATE,TIME,OPEN,HIGH,LOW,CLOSE,VOL

20240912,100000,90300,90340,90250,90321,516

20240912,100100,90318,90401,90302,90380,165

20240912,100200,90380,90380,90325,90330,72

20240912,100300,90338,90371,90309,90315,126

20240912,100400,90326,90373,90317,90346,109

Where:

DATE: YYYYMMDD (e.g., 20240912)

TIME: HHMMSS (e.g., 100000)

OPEN, HIGH, LOW, CLOSE: f64 prices

VOL: u64 volume

 ⚠️ Files must have headers. No extra columns or comments. 

---

## 🗂 File Structure

After conversion:

output.bin       ← FlatBuffer binary (OHLCVList)

output.idx       ← Bincode-serialized FullIndex

.idx contains:

time_index: [timestamp, index] for every bar

daily_index: [date, start_index, end_index]

timeframe_index: [timestamps] for every N-minute bar

---

## 🧪 Example Output

📄 Read first 5 OHLCV entries

 - ts: 20231214 090000, open: 90302.00, high: 90399.00, low: 90120.00, close: 90265.00, vol: 1320
 - ts: 20231214 090100, open: 90252.00, high: 90255.00, low: 90224.00, close: 90234.00, vol: 154
 ...
 
✅ Reading files complete in 0.05 seconds

---

## 📈 Why FlatBuffers?

✅ Zero-copy deserialization : Access data directly from memory

✅ Schema evolution : Safe versioning

✅ Cross-language : Use .bin files in Python, C++, JS, etc.

✅ Compact & fast : Ideal for large datasets

---

## 🧩 Integration with Backtester

Use .bin + .idx files in your event-driven backtester:

Load only needed days

Resample on-demand

Ultra-low-latency bar updates

---

## Future roadmap:

Columnar storage

SIMD aggregation

WebSocket live feed support

---

## 📄 License

MIT
