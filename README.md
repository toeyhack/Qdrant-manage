# กรณีที่ Qdrant ไม่มี API Key
1. แสดง collection ทั้งหมด
python qdrant_manage_3.py --host ip --port 6333 --list

2. ดูสรุปเอกสาร (group by doc_id)
python qdrant_manage_3.py --host ip --port 6333 --collection documents --view

3. Inspect payload เต็ม
python qdrant_manage_3.py --host ip --port 6333 --collection documents --inspect --limit 5

4. ลบเอกสารเฉพาะ doc_id = "report_01"
python qdrant_manage_3.py --host ip --port 6333 \
  --collection documents --delete-chunk \
  --chunk-field doc_id --value report_01 --yes

5. ลบทั้งหมดใน collection
python qdrant_manage_3.py --host ip --port 6333 --collection documents --delete-all --yes
# กรณีที่ Qdrant มี API Key
python qdrant_manage_3.py \
  --host ip \
  --port 6333 \
  --api-key YOUR_SECRET_KEY \
  --inspect \
  --collection documents


หรือถ้าเป็น HTTPS endpoint เช่นของ Qdrant Cloud:

python qdrant_manage_3.py \
  --host my-cluster-12345.qdrant.cloud \
  --https \
  --api-key sk_example_api_key_123456789 \
  --inspect \
  --collection documents
