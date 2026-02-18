# Insolventz v4 (clean architecture)

Це «з нуля» clean-реалізація беклогу (v4):
- `projects/dataroom/` як єдиний storage для кейсів і БД
- SQLite БД: `projects/dataroom/insolventz_database.db` (створюється автоматично)
- 4 вкладки UI: Case Settings / Document Management / Transactions / Notices
- Upload документів у правильні папки `source_info/*`
- Парсинг bank statements у форматах CSV/XLS(X)/PDF(text)/PDF(scan-ocr fallback)
- Дедуплікація транзакцій через `tx_hash` (ключ: source_account + recipient_account + recipient_name + amount + description)
- Фільтри/сортування транзакцій + редагування tags (зберігається в БД)
- Генерація notice по вибраним транзакціям, групування по контрагенту
- Статуси notice: Generated / Accepted / Sent
- Посилання на source файли і generated PDF

## Запуск

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Відкрити: http://localhost:8000

## Де лежать дані

- `projects/dataroom/insolventz_database.db`
- `projects/dataroom/cases/<case_id>/source_info/...`
- `projects/dataroom/cases/<case_id>/notices/...`

## Примітки

OCR для сканованих PDF потребує системних залежностей:
- Tesseract
- Poppler (для `pdf2image`)

Якщо OCR/Poppler відсутні — text-PDF працюватиме, а scan-PDF може повернути помилку.

### OCR-ready workflow (v5)

- При upload PDF система робить **авто-детекцію** text-layer.
- Якщо text-layer відсутній → документ переходить у статус **`ocr_required`**.
- У списку документів з'являється кнопка **Run OCR**.
- OCR запускається у background task і оновлює **`ocr_progress` (0..100)**.
- Після завершення: статус **`ocr_done`**, а витягнутий текст зберігається як `*.ocr.txt` поруч із PDF.

#### Windows (PATH / Tesseract)

Якщо Tesseract не доступний через PATH, можна задати шлях через env:

PowerShell:

```powershell
$env:TESSERACT_CMD = "C:\\Program Files\\Tesseract-OCR\\tesseract.exe"
```
