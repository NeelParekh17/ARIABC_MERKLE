DROP TABLE IF EXISTS public.safe_phase3_probe;

CREATE TABLE public.safe_phase3_probe (
    k integer PRIMARY KEY,
    n integer NOT NULL,
    token text NOT NULL DEFAULT ''
);

INSERT INTO public.safe_phase3_probe (k, n, token)
SELECT i, 0, ''
FROM generate_series(1, 50) AS i;
