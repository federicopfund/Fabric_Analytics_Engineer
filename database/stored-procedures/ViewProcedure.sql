SELECT 
    proname AS procedure_name,
    prosrc AS source_code
FROM 
    pg_proc
WHERE 
    pronamespace = (
        SELECT oid 
        FROM pg_namespace 
        WHERE nspname = 'public'
    );
