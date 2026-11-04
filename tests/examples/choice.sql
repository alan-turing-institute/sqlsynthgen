-- DROP DATABASE IF EXISTS instrument WITH (FORCE);
CREATE DATABASE numbers WITH TEMPLATE template0 ENCODING = 'UTF8' ;
ALTER DATABASE numbers OWNER TO postgres;

\connect numbers

CREATE TABLE public.number_table (
    id INTEGER NOT NULL,
    one INTEGER NOT NULL,
    two INTEGER NOT NULL,
    three INTEGER NOT NULL
);

ALTER TABLE ONLY public.number_table ADD CONSTRAINT number_table_pkey PRIMARY KEY (id);

ALTER TABLE public.number_table OWNER TO postgres;

INSERT INTO public.number_table VALUES (1, 1, 1, 1);
INSERT INTO public.number_table VALUES (2, 2, 2, 2);
INSERT INTO public.number_table VALUES (3, 3, 3, 3);
INSERT INTO public.number_table VALUES (4, 4, 4, 4);
INSERT INTO public.number_table VALUES (5, 5, 5, 5);
INSERT INTO public.number_table VALUES (6, 1, 1, 1);
INSERT INTO public.number_table VALUES (7, 1, 2, 2);
INSERT INTO public.number_table VALUES (8, 1, 3, 3);
INSERT INTO public.number_table VALUES (9, 1, 3, 4);
INSERT INTO public.number_table VALUES (10, 1, 3, 5);
INSERT INTO public.number_table VALUES (11, 1, 2, 1);
INSERT INTO public.number_table VALUES (12, 1, 2, 2);
INSERT INTO public.number_table VALUES (13, 4, 1, 3);
INSERT INTO public.number_table VALUES (14, 4, 3, 4);
INSERT INTO public.number_table VALUES (15, 1, 3, 5);
INSERT INTO public.number_table VALUES (16, 1, 2, 1);
INSERT INTO public.number_table VALUES (17, 4, 3, 2);
INSERT INTO public.number_table VALUES (18, 4, 2, 3);
INSERT INTO public.number_table VALUES (19, 4, 3, 4);
INSERT INTO public.number_table VALUES (20, 4, 1, 5);
