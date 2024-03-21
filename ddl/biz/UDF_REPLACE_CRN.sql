CREATE OR REPLACE FUNCTION PUBLIC.UDF_REPLACE_CRN(Convert_param VARCHAR)
RETURNS VARCHAR AS $$
BEGIN
    RETURN TRIM(regexp_replace(regexp_replace(regexp_replace(regexp_replace(COALESCE(Convert_param, ''), '\n', '','g'), '\r', ''), '\s\s*', ' ','g'), '\xa0', ''));
END;
$$ LANGUAGE plpgsql;
