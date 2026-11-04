-- DROP DATABASE IF EXISTS tricky WITH (FORCE);
CREATE DATABASE tricky WITH TEMPLATE template0 ENCODING = 'UTF8' ;
ALTER DATABASE tricky OWNER TO postgres;

\connect tricky

CREATE TABLE public.names (
    id INTEGER NOT NULL,
    "offset" INTEGER,
    "count" INTEGER NOT NULL,
    sensible TEXT
);

ALTER TABLE ONLY public.names ADD CONSTRAINT names_pkey PRIMARY KEY (id);

ALTER TABLE public.names OWNER TO postgres;

INSERT INTO public.names VALUES (1, 10, 5, 'reasonable');
INSERT INTO public.names VALUES (2, NULL, 6, 'clear-headed');
