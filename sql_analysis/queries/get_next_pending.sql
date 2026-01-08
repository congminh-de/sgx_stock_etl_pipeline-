SELECT file_id
FROM meta_file_status
WHERE status = 'PENDING'
ORDER BY file_id ASC
LIMIT 1;