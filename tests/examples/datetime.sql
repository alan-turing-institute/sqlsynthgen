-- DROP DATABASE IF EXISTS date_time_extract WITH (FORCE);
CREATE DATABASE date_time_extract WITH TEMPLATE template0 ENCODING = 'UTF8' ;
ALTER DATABASE date_time_extract OWNER TO postgres;

\connect date_time_extract

CREATE TABLE public.dates (
    id INTEGER NOT NULL,
    "year" INTEGER,
    "month" INTEGER,
    "day" INTEGER,
    "date" DATE,
    "getfrom" TIMESTAMP WITH TIME ZONE NOT NULL,
    "orfrom" TIMESTAMP WITH TIME ZONE NOT NULL
);

ALTER TABLE ONLY public.dates ADD CONSTRAINT dates_pkey PRIMARY KEY (id);

ALTER TABLE public.dates OWNER TO postgres;

INSERT INTO public.dates VALUES (1, 1951, 1, 8, '1951-01-08', '1951-01-08 12:05:06+00:00', '1953-03-28 11:25:26+00:00');
INSERT INTO public.dates VALUES (2, 2021, 12, 3, '2021-12-03', '2021-12-03 15:41:12+00:00', '2022-03-18 16:25:27+00:00');
