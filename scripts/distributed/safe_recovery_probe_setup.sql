DROP TABLE IF EXISTS public.safe_recovery_probe;

CREATE TABLE public.safe_recovery_probe (
    k integer PRIMARY KEY,
    n integer NOT NULL,
    token text NOT NULL DEFAULT ''
);

INSERT INTO public.safe_recovery_probe (k, n, token)
VALUES (1, 0, '');
