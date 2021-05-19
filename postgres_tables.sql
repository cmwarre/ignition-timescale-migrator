--
-- PostgreSQL database dump
--
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
-- Dumped from database version 13.3 (Ubuntu 13.3-1.pgdg18.04+1)
-- Dumped by pg_dump version 13.3 (Ubuntu 13.3-1.pgdg18.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: sqlth_1_data; Type: TABLE; Schema: public; Owner: ignition
--

CREATE TABLE if not exists public.sqlth_1_data (
    tagid integer NOT NULL,
    intvalue bigint,
    floatvalue double precision,
    stringvalue character varying(255),
    datevalue timestamp without time zone,
    dataintegrity integer,
    t_stamp bigint NOT NULL
);

-- Name: sqlth_annotations; Type: TABLE; Schema: public; Owner: ignition
--

CREATE TABLE if not exists public.sqlth_annotations (
    id integer NOT NULL,
    tagid integer,
    start_time bigint,
    end_time bigint,
    type character varying(255),
    datavalue character varying(255),
    annotationid character varying(255)
);

--
-- Name: sqlth_annotations_id_seq; Type: SEQUENCE; Schema: public; Owner: ignition
--

CREATE SEQUENCE if not exists public.sqlth_annotations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: sqlth_annotations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ignition
--

ALTER SEQUENCE public.sqlth_annotations_id_seq OWNED BY public.sqlth_annotations.id;


--
-- Name: sqlth_drv; Type: TABLE; Schema: public; Owner: ignition
--

CREATE TABLE if not exists public.sqlth_drv (
    id integer NOT NULL,
    name character varying(255),
    provider character varying(255)
);

--
-- Name: sqlth_drv_id_seq; Type: SEQUENCE; Schema: public; Owner: ignition
--

CREATE SEQUENCE if not exists public.sqlth_drv_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: sqlth_drv_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ignition
--

ALTER SEQUENCE public.sqlth_drv_id_seq OWNED BY public.sqlth_drv.id;


--
-- Name: sqlth_partitions; Type: TABLE; Schema: public; Owner: ignition
--

CREATE TABLE if not exists public.sqlth_partitions (
    pname character varying(255),
    drvid integer,
    start_time bigint,
    end_time bigint,
    blocksize integer,
    flags integer
);

--
-- Name: sqlth_sce; Type: TABLE; Schema: public; Owner: ignition
--

CREATE TABLE if not exists public.sqlth_sce (
    scid integer,
    start_time bigint,
    end_time bigint,
    rate integer
);

--
-- Name: sqlth_scinfo; Type: TABLE; Schema: public; Owner: ignition
--

CREATE TABLE if not exists public.sqlth_scinfo (
    id integer NOT NULL,
    scname character varying(255),
    drvid integer
);

--
-- Name: sqlth_scinfo_id_seq; Type: SEQUENCE; Schema: public; Owner: ignition
--

CREATE SEQUENCE if not exists public.sqlth_scinfo_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: sqlth_scinfo_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ignition
--

ALTER SEQUENCE public.sqlth_scinfo_id_seq OWNED BY public.sqlth_scinfo.id;


--
-- Name: sqlth_te; Type: TABLE; Schema: public; Owner: ignition
--

CREATE TABLE if not exists public.sqlth_te (
    id integer NOT NULL,
    tagpath character varying(255),
    scid integer,
    datatype integer,
    querymode integer,
    created bigint,
    retired bigint
);

--
-- Name: sqlth_te_id_seq; Type: SEQUENCE; Schema: public; Owner: ignition
--

CREATE SEQUENCE IF NOT EXISTS public.sqlth_te_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: sqlth_te_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: ignition
--

ALTER SEQUENCE public.sqlth_te_id_seq OWNED BY public.sqlth_te.id;


--
-- Name: sqlth_annotations id; Type: DEFAULT; Schema: public; Owner: ignition
--

ALTER TABLE ONLY public.sqlth_annotations ALTER COLUMN id SET DEFAULT nextval('public.sqlth_annotations_id_seq'::regclass);


--
-- Name: sqlth_drv id; Type: DEFAULT; Schema: public; Owner: ignition
--

ALTER TABLE ONLY public.sqlth_drv ALTER COLUMN id SET DEFAULT nextval('public.sqlth_drv_id_seq'::regclass);


--
-- Name: sqlth_scinfo id; Type: DEFAULT; Schema: public; Owner: ignition
--

ALTER TABLE ONLY public.sqlth_scinfo ALTER COLUMN id SET DEFAULT nextval('public.sqlth_scinfo_id_seq'::regclass);


--
-- Name: sqlth_te id; Type: DEFAULT; Schema: public; Owner: ignition
--

ALTER TABLE ONLY public.sqlth_te ALTER COLUMN id SET DEFAULT nextval('public.sqlth_te_id_seq'::regclass);

--
-- Name: sqlth_1_data sqlth_1_data_pkey; Type: CONSTRAINT; Schema: public; Owner: ignition
--

ALTER TABLE ONLY public.sqlth_1_data
    ADD CONSTRAINT sqlth_1_data_pkey PRIMARY KEY (tagid, t_stamp);


--
-- Name: sqlth_annotations sqlth_annotations_pkey; Type: CONSTRAINT; Schema: public; Owner: ignition
--

ALTER TABLE ONLY public.sqlth_annotations
    ADD CONSTRAINT sqlth_annotations_pkey PRIMARY KEY (id);


--
-- Name: sqlth_drv sqlth_drv_pkey; Type: CONSTRAINT; Schema: public; Owner: ignition
--

ALTER TABLE ONLY public.sqlth_drv
    ADD CONSTRAINT sqlth_drv_pkey PRIMARY KEY (id);


--
-- Name: sqlth_scinfo sqlth_scinfo_pkey; Type: CONSTRAINT; Schema: public; Owner: ignition
--

ALTER TABLE ONLY public.sqlth_scinfo
    ADD CONSTRAINT sqlth_scinfo_pkey PRIMARY KEY (id);


--
-- Name: sqlth_te sqlth_te_pkey; Type: CONSTRAINT; Schema: public; Owner: ignition
--

ALTER TABLE ONLY public.sqlth_te
    ADD CONSTRAINT sqlth_te_pkey PRIMARY KEY (id);


--
-- Name: sqlth_1_datat_stampndx; Type: INDEX; Schema: public; Owner: ignition
--

CREATE INDEX if not exists sqlth_1_datat_stampndx ON public.sqlth_1_data USING btree (t_stamp);


--
-- Name: sqlth_annotationsend_timendx; Type: INDEX; Schema: public; Owner: ignition
--

CREATE INDEX if not exists sqlth_annotationsend_timendx ON public.sqlth_annotations USING btree (end_time);


--
-- Name: sqlth_sceend_timendx; Type: INDEX; Schema: public; Owner: ignition
--

CREATE INDEX if not exists sqlth_sceend_timendx ON public.sqlth_sce USING btree (end_time);


--
-- Name: sqlth_scestart_timendx; Type: INDEX; Schema: public; Owner: ignition
--

CREATE INDEX if not exists sqlth_scestart_timendx ON public.sqlth_sce USING btree (start_time);


--
-- Name: sqlth_tetagpathndx; Type: INDEX; Schema: public; Owner: ignition
--

CREATE INDEX if not exists sqlth_tetagpathndx ON public.sqlth_te USING btree (tagpath);


--
-- Name: sqlthannotationsstarttimendx; Type: INDEX; Schema: public; Owner: ignition
--

CREATE INDEX if not exists sqlthannotationsstarttimendx ON public.sqlth_annotations USING btree (start_time);

