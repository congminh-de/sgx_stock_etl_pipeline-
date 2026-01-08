UPDATE meta_file_status
SET status = :status,
    updated_at = NOW(),
    retry_count = CASE 
        WHEN :status = 'FAILED' THEN retry_count + 1 
        ELSE retry_count 
    END 
WHERE file_id = :file_id;