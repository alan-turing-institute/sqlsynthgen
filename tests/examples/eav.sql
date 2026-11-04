-- DROP DATABASE IF EXISTS instrument WITH (FORCE);
CREATE DATABASE eav WITH TEMPLATE template0 ENCODING = 'UTF8' ;
ALTER DATABASE eav OWNER TO postgres;

\connect eav

CREATE TABLE public.measurement_type (
    id INTEGER NOT NULL,
    name TEXT
);

ALTER TABLE ONLY public.measurement_type ADD CONSTRAINT measurement_type_pkey PRIMARY KEY (id);

ALTER TABLE public.measurement_type OWNER TO postgres;

INSERT INTO public.measurement_type VALUES (1, 'agreement');
INSERT INTO public.measurement_type VALUES (2, 'acceleration');
INSERT INTO public.measurement_type VALUES (3, 'velocity');
INSERT INTO public.measurement_type VALUES (4, 'position');
INSERT INTO public.measurement_type VALUES (5, 'matter');

CREATE TABLE public.measurement (
    id INTEGER NOT NULL,
    type INTEGER NOT NULL,
    first_value FLOAT,
    second_value FLOAT,
    third_value TEXT
);

ALTER TABLE ONLY public.measurement ADD CONSTRAINT measurement_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.measurement
    ADD CONSTRAINT measurement_type_fkey FOREIGN KEY (type) REFERENCES public.measurement_type(id);

ALTER TABLE public.measurement OWNER TO postgres;

INSERT INTO public.measurement VALUES (1, 1, NULL, NULL, 'yes');
INSERT INTO public.measurement VALUES (2, 1, NULL, NULL, 'yes');
INSERT INTO public.measurement VALUES (3, 1, NULL, NULL, 'no');
INSERT INTO public.measurement VALUES (4, 1, NULL, NULL, 'no');
INSERT INTO public.measurement VALUES (5, 1, NULL, NULL, 'no');
INSERT INTO public.measurement VALUES (6, 2, 1.0, 1.5, NULL);
INSERT INTO public.measurement VALUES (7, 2, 1.3, 1.9, NULL);
INSERT INTO public.measurement VALUES (8, 2, 1.9, 2.0, NULL);
INSERT INTO public.measurement VALUES (9, 3, 10.1, 13.2, NULL);
INSERT INTO public.measurement VALUES (10, 3, 12.0, 12.6, NULL);
INSERT INTO public.measurement VALUES (11, 3, 13.3, 10.5, NULL);
INSERT INTO public.measurement VALUES (12, 4, 20.3, 20.1, NULL);
INSERT INTO public.measurement VALUES (13, 4, 22.4, 26.4, NULL);
INSERT INTO public.measurement VALUES (14, 4, 21.5, 23.7, NULL);
INSERT INTO public.measurement VALUES (15, 5, 7.4, NULL, 'fish');
INSERT INTO public.measurement VALUES (16, 5, 8.0, NULL, 'fish');
INSERT INTO public.measurement VALUES (17, 5, 8.9, NULL, 'fish');
INSERT INTO public.measurement VALUES (18, 5, 10.2, NULL, 'fowl');
INSERT INTO public.measurement VALUES (19, 5, 11.0, NULL, 'fowl');
INSERT INTO public.measurement VALUES (20, 5, 12.4, NULL, 'fowl');

CREATE TABLE public.observation (
    id INTEGER NOT NULL,
    type INTEGER NOT NULL,
    first_value FLOAT,
    second_value FLOAT,
    third_value TEXT
);

ALTER TABLE ONLY public.observation ADD CONSTRAINT observation_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.observation
    ADD CONSTRAINT observation_type_fkey FOREIGN KEY (type) REFERENCES public.measurement_type(id);

ALTER TABLE public.observation OWNER TO postgres;

INSERT INTO public.observation VALUES (1, 1, 1.2, NULL, 'ham');
INSERT INTO public.observation VALUES (2, 1, 1.3, NULL, 'eggs');
INSERT INTO public.observation VALUES (3, 1, 1.4, NULL, 'ham');
INSERT INTO public.observation VALUES (4, 1, 1.3, NULL, 'eggs');
INSERT INTO public.observation VALUES (5, 1, 1.5, NULL, 'eggs');
INSERT INTO public.observation VALUES (6, 1, 9.2, NULL, 'cheese');
INSERT INTO public.observation VALUES (7, 1, 9.3, NULL, 'cheese');
INSERT INTO public.observation VALUES (8, 1, 1.1, NULL, 'ham');

CREATE TABLE public."double" (
    id INTEGER NOT NULL,
    type INTEGER NOT NULL,
    subtype INTEGER NOT NULL,
    value TEXT
);

ALTER TABLE ONLY public."double" ADD CONSTRAINT double_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public."double"
    ADD CONSTRAINT double_type_fkey FOREIGN KEY (type) REFERENCES public.measurement_type(id);
ALTER TABLE ONLY public."double"
    ADD CONSTRAINT double_subtype_fkey FOREIGN KEY (subtype) REFERENCES public.measurement_type(id);

ALTER TABLE public."double" OWNER TO postgres;

INSERT INTO public."double" VALUES (1, 1, 1, 'yes');
INSERT INTO public."double" VALUES (2, 2, 1, 'maybe');
INSERT INTO public."double" VALUES (3, 2, 4, 'no');
