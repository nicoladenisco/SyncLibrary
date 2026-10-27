
CREATE TABLE origin_siti
(
    siti_id integer NOT NULL,
    codice character varying(255) COLLATE pg_catalog."default" NOT NULL,
    descrizione character varying(255) COLLATE pg_catalog."default" NOT NULL,
    stato_rec integer DEFAULT 0,
    id_user integer DEFAULT 0,
    id_ucrea integer DEFAULT 0,
    ult_modif timestamp without time zone,
    creazione timestamp without time zone,
    CONSTRAINT origin_siti_pkey PRIMARY KEY (siti_id),
    CONSTRAINT idx_stp_origin_siti_1 UNIQUE (codice)
);

CREATE TABLE target_siti
(
    siti_id integer NOT NULL,
    codice character varying(255) COLLATE pg_catalog."default" NOT NULL,
    descrizione character varying(255) COLLATE pg_catalog."default" NOT NULL,
    stato_rec integer DEFAULT 0,
    id_user integer DEFAULT 0,
    id_ucrea integer DEFAULT 0,
    ult_modif timestamp without time zone,
    creazione timestamp without time zone,
    CONSTRAINT target_siti_pkey PRIMARY KEY (siti_id),
    CONSTRAINT idx_stp_target_siti_1 UNIQUE (codice)
);

CREATE TABLE origin_reparti
(
    reparti_id integer NOT NULL,
    codice character varying(255) COLLATE pg_catalog."default" NOT NULL,
    descrizione character varying(255) COLLATE pg_catalog."default" NOT NULL,
    stato_rec integer DEFAULT 0,
    id_user integer DEFAULT 0,
    id_ucrea integer DEFAULT 0,
    ult_modif timestamp without time zone,
    creazione timestamp without time zone,
    CONSTRAINT origin_reparti_pkey PRIMARY KEY (reparti_id),
    CONSTRAINT idx_stp_origin_reparti_1 UNIQUE (codice)
);

CREATE TABLE target_reparti
(
    reparti_id integer NOT NULL,
    codice character varying(255) COLLATE pg_catalog."default" NOT NULL,
    descrizione character varying(255) COLLATE pg_catalog."default" NOT NULL,
    stato_rec integer DEFAULT 0,
    id_user integer DEFAULT 0,
    id_ucrea integer DEFAULT 0,
    ult_modif timestamp without time zone,
    creazione timestamp without time zone,
    CONSTRAINT target_reparti_pkey PRIMARY KEY (reparti_id),
    CONSTRAINT idx_stp_target_reparti_1 UNIQUE (codice)
);

CREATE TABLE origin_zone
(
    zone_id integer NOT NULL,
    codice character varying(255) COLLATE pg_catalog."default" NOT NULL,
    descrizione character varying(255) COLLATE pg_catalog."default" NOT NULL,
    stato_rec integer DEFAULT 0,
    id_user integer DEFAULT 0,
    id_ucrea integer DEFAULT 0,
    ult_modif timestamp without time zone,
    creazione timestamp without time zone,
    CONSTRAINT origin_zone_pkey PRIMARY KEY (zone_id),
    CONSTRAINT idx_stp_origin_zone_1 UNIQUE (codice)
);

CREATE TABLE target_zone
(
    zone_id integer NOT NULL,
    codice character varying(255) COLLATE pg_catalog."default" NOT NULL,
    descrizione character varying(255) COLLATE pg_catalog."default" NOT NULL,
    stato_rec integer DEFAULT 0,
    id_user integer DEFAULT 0,
    id_ucrea integer DEFAULT 0,
    ult_modif timestamp without time zone,
    creazione timestamp without time zone,
    CONSTRAINT target_zone_pkey PRIMARY KEY (zone_id),
    CONSTRAINT idx_stp_target_zone_1 UNIQUE (codice)
);

CREATE TABLE origin_stampanti
(
    stampanti_id integer NOT NULL,
    codice character varying(32) COLLATE pg_catalog."default",
    descrizione character varying(255) COLLATE pg_catalog."default",
    tipo character varying(50) COLLATE pg_catalog."default",
    ipclient character varying(100) COLLATE pg_catalog."default",
    nomeclient character varying(100) COLLATE pg_catalog."default",
    percorsostampante character varying(100) COLLATE pg_catalog."default",
    template1 character varying(100) COLLATE pg_catalog."default",
    template2 character varying(100) COLLATE pg_catalog."default",
    ordinestampa integer DEFAULT 0,
    esamiperetic integer DEFAULT 0,
    stato_rec integer DEFAULT 0,
    id_user integer DEFAULT 0,
    id_ucrea integer DEFAULT 0,
    ult_modif timestamp without time zone,
    creazione timestamp without time zone,
    CONSTRAINT origin_stampanti_pkey PRIMARY KEY (stampanti_id)
);

CREATE TABLE target_stampanti
(
    stampanti_id integer NOT NULL,
    codice character varying(32) COLLATE pg_catalog."default",
    descrizione character varying(255) COLLATE pg_catalog."default",
    tipo character varying(50) COLLATE pg_catalog."default",
    ipclient character varying(100) COLLATE pg_catalog."default",
    nomeclient character varying(100) COLLATE pg_catalog."default",
    percorsostampante character varying(100) COLLATE pg_catalog."default",
    template1 character varying(100) COLLATE pg_catalog."default",
    template2 character varying(100) COLLATE pg_catalog."default",
    ordinestampa integer DEFAULT 0,
    esamiperetic integer DEFAULT 0,
    stato_rec integer DEFAULT 0,
    id_user integer DEFAULT 0,
    id_ucrea integer DEFAULT 0,
    ult_modif timestamp without time zone,
    creazione timestamp without time zone,
    CONSTRAINT target_stampanti_pkey PRIMARY KEY (stampanti_id)
);

CREATE TABLE origin_tipietichette
(
    tipietichette_id integer NOT NULL,
    codice character varying(32) COLLATE pg_catalog."default",
    descrizione character varying(255) COLLATE pg_catalog."default",
    id_stampante_default integer DEFAULT 0,
    capacita double precision,
    colore character varying(10) COLLATE pg_catalog."default",
    stato_rec integer DEFAULT 0,
    id_user integer DEFAULT 0,
    id_ucrea integer DEFAULT 0,
    ult_modif timestamp without time zone,
    creazione timestamp without time zone,
    CONSTRAINT origin_tipietichette_pkey PRIMARY KEY (tipietichette_id),
    CONSTRAINT idx_stp_origin_tipietichette_1 UNIQUE (codice),
    CONSTRAINT origin_tipietichette_fk_1 FOREIGN KEY (id_stampante_default)
        REFERENCES stp.stampanti (stampanti_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
);

CREATE TABLE target_tipietichette
(
    tipietichette_id integer NOT NULL,
    codice character varying(32) COLLATE pg_catalog."default",
    descrizione character varying(255) COLLATE pg_catalog."default",
    id_stampante_default integer DEFAULT 0,
    capacita double precision,
    colore character varying(10) COLLATE pg_catalog."default",
    stato_rec integer DEFAULT 0,
    id_user integer DEFAULT 0,
    id_ucrea integer DEFAULT 0,
    ult_modif timestamp without time zone,
    creazione timestamp without time zone,
    CONSTRAINT target_tipietichette_pkey PRIMARY KEY (tipietichette_id),
    CONSTRAINT idx_stp_target_tipietichette_1 UNIQUE (codice),
    CONSTRAINT target_tipietichette_fk_1 FOREIGN KEY (id_stampante_default)
        REFERENCES target_stampanti (stampanti_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE CASCADE
);

