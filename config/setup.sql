CREATE TABLE IF NOT EXISTS public.fraud_payments (
 id BIGINT PRIMARY KEY,
 name VARCHAR(100) NOT NULL,
 amount NUMERIC(12,2) NOT NULL CHECK (amount >= 0)
);
GRANT USAGE ON SCHEMA public TO connect_user;
GRANT SELECT ON public.fraud_payments TO connect_user;
