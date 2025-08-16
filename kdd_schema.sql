--- By: Bruno Tenório Ávila
--- Acesse: https://agade.ufpe.br/agade_mineracao.php

--------------------------------------
--  Esquema, linguagem e extensões  --
--------------------------------------

CREATE SCHEMA IF NOT EXISTS kdd
    AUTHORIZATION postgres;
	

----------------------------
--  Estrutura do itemset  --
----------------------------

DO $$
BEGIN
  -- cria se não existir
  IF NOT EXISTS (
    SELECT 1 FROM pg_type WHERE typname = 'apriori_itemset'
  ) THEN

    -- itemset
	CREATE TYPE kdd.apriori_itemset AS
	(
		id_atributo integer,
		id_item integer
	);
	-- itemset

  END IF;
END;
$$;


-----------------------
--  Cria as tabelas  --
-----------------------
	
CREATE TABLE IF NOT EXISTS kdd.processos
(
    id_processo integer NOT NULL,
    titulo character varying(1000) NOT NULL,
    descricao text NOT NULL,
	inicio timestamp without time zone,
    fim timestamp without time zone,
    CONSTRAINT pk_processos PRIMARY KEY (id_processo)
);

CREATE TABLE IF NOT EXISTS kdd.atributos
(
    id_processo integer NOT NULL,
    id_atributo integer NOT NULL,
    nome character varying(200) NOT NULL,
	tipo character(1) NOT NULL,  -- 'C' para categórico, 'N' para numérico
    CONSTRAINT pk_atributos PRIMARY KEY (id_atributo, id_processo),
    CONSTRAINT un_atributos_id_processo_nome UNIQUE (id_processo, nome),
    CONSTRAINT fk_atributos_id_processo FOREIGN KEY (id_processo)
        REFERENCES kdd.processos (id_processo) MATCH SIMPLE
        ON UPDATE RESTRICT ON DELETE RESTRICT
);
	
CREATE TABLE IF NOT EXISTS kdd.itens
(
    id_processo integer NOT NULL,
	id_item integer NOT NULL,
	id_atributo integer NOT NULL,
    nome character varying(200) NOT NULL,
    CONSTRAINT pk_itens PRIMARY KEY (id_processo, id_atributo, id_item),
    CONSTRAINT un_itens_id_processo_id_atributo_nome 
	  UNIQUE (id_processo, id_atributo, nome),
    CONSTRAINT fk_itens_id_processo FOREIGN KEY (id_processo)
        REFERENCES kdd.processos (id_processo) MATCH SIMPLE
        ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS kdd.transacoes
(
    id_processo integer NOT NULL,
	id_transacao integer NOT NULL,
    id_atributo integer NOT NULL,
	id_item integer NOT NULL,
    CONSTRAINT pk_transacoes PRIMARY KEY (id_processo, id_transacao, id_atributo, id_item),
    CONSTRAINT fk_transacoes_id_processo_id_atributo_id_item 
	    FOREIGN KEY (id_processo, id_atributo, id_item)
        REFERENCES kdd.itens (id_processo, id_atributo, id_item) MATCH SIMPLE
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT fk_transacoes_id_processo FOREIGN KEY (id_processo)
        REFERENCES kdd.processos (id_processo) MATCH SIMPLE
        ON UPDATE RESTRICT ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS kdd.regras ( 
    id_regra serial primary key,
    id_processo integer not null,
    antecedente text not null, 
    consequente text not null, 
    confianca float not null, 
    suporte float not null, 
    lift float not null );  
;


CREATE TABLE IF NOT EXISTS kdd.arvore_decisao
(
    id_processo integer NOT NULL,
    unique_node_id serial,
    node_id integer,
    parent_node_id integer,
    parent_value text,
    split_attribute text,
    split_value text,
    is_leaf boolean,
    class_value text,
    class_count integer,
    total_count integer,
    node_depth integer,
    information_gain double precision,
    CONSTRAINT pk_arvore_decisao PRIMARY KEY (id_processo, unique_node_id),
    CONSTRAINT fk_arvore_decisao_id_processo FOREIGN KEY (id_processo)
        REFERENCES kdd.processos (id_processo) MATCH SIMPLE
        ON UPDATE NO ACTION ON DELETE NO ACTION
);


---------------------------------------
--  Início e fim do processo de kdd  --
---------------------------------------

CREATE OR REPLACE FUNCTION kdd.util_iniciar_processo_kdd(p_id_processo integer) 
  RETURNS void AS $$
DECLARE
  v_titulo character varying;
  v_linha character varying;
  i integer;
  v_timestamp timestamptz;
BEGIN
  -- registro do início do processo
  v_timestamp := clock_timestamp();
  UPDATE kdd.processos SET inicio = v_timestamp WHERE id_processo = p_id_processo;

  -- cabeçalho do processo
  SELECT '--  PROCESSO DE KDD' || to_char(p_id_processo, '09') || ': ' || titulo || '  --' 
    INTO v_titulo FROM kdd.processos WHERE id_processo = p_id_processo;
  v_linha := '';
  FOR i IN 1..length(v_titulo) LOOP
    v_linha := v_linha || '-';
  END LOOP;
  RAISE NOTICE '%', v_linha;
  RAISE NOTICE '%', v_titulo;
  RAISE NOTICE '%', v_linha;

  -- interface do início da etapa de pré-processamento
  RAISE NOTICE '';
  RAISE NOTICE 'PRÉ-PROCESSAMENTO';
  RAISE NOTICE '-----------------';
  RAISE NOTICE '';

  -- limpa as tabelas usadas pelos algoritmos de mineração
  DELETE FROM kdd.transacoes     WHERE id_processo = p_id_processo;
  DELETE FROM kdd.itens		     WHERE id_processo = p_id_processo;
  DELETE FROM kdd.atributos      WHERE id_processo = p_id_processo;
  DELETE FROM kdd.regras 	     WHERE id_processo = p_id_processo;
  DELETE FROM kdd.textos         WHERE id_processo = p_id_processo;
  DELETE FROM kdd.regressoes     WHERE id_processo = p_id_processo;
  DELETE FROM kdd.arvore_decisao WHERE id_processo = p_id_processo;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION kdd.util_finalizar_processo_kdd(p_id_processo integer) 
  RETURNS void AS $$
DECLARE
  v_timestamp timestamptz;
BEGIN
  v_timestamp := clock_timestamp();
  UPDATE kdd.processos SET fim = v_timestamp WHERE id_processo = p_id_processo;
END;
$$ LANGUAGE plpgsql;


--------------------------------------------
--  Início da etapa de pós-processamento  --
--------------------------------------------

CREATE OR REPLACE FUNCTION kdd.util_iniciar_pos_processamento() 
  RETURNS void AS $$
BEGIN
  RAISE NOTICE '';
  RAISE NOTICE 'PÓS-PROCESSAMENTO';
  RAISE NOTICE '-----------------';
  RAISE NOTICE '';
END;
$$ LANGUAGE plpgsql;


------------------------------------------------------
--  Cria um identificador inteiro para uma palavra  --
------------------------------------------------------

CREATE OR REPLACE FUNCTION kdd.util_hash_text(s text) 
  RETURNS integer AS $$
BEGIN
    IF s IS NULL THEN
        RETURN NULL;
    ELSE
        -- Calcula o hash
        RETURN ABS(('x' || substr(md5(s),1,8))::bit(32)::integer);
    END IF;
END;
$$ LANGUAGE plpgsql;


---------------------------------------------------------------------------------
--  Agrupa os objetos de acordo com o número de páginas e o ano de publicação  --
---------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION kdd.util_discretizar(p integer, q integer) 
  RETURNS integer AS $$
BEGIN
  RETURN FLOOR (p / q) * q;
END;
$$ LANGUAGE plpgsql;


---------------------------------
--  Formata número para texto  --
---------------------------------

CREATE OR REPLACE FUNCTION kdd.util_formatar_numero(numero numeric, precisao integer)
  RETURNS text AS $$
DECLARE
  formato text := '';
BEGIN
  IF precisao = 0 THEN
    formato := 'FM999G999G999G999G999';
  ELSE
    formato := 'FM999G999G999G999G990D' || REPEAT('0', precisao);
  END IF;

  RETURN to_char(numero, formato);
END;
$$ LANGUAGE plpgsql;


------------------------------------
-- Computa o total de combinações --
------------------------------------

CREATE OR REPLACE FUNCTION kdd.util_combinacoes_total(n integer, k integer)
RETURNS NUMERIC AS $$
BEGIN
  RETURN factorial(n) / (factorial(n - k) * factorial(k));
END;
$$ LANGUAGE plpgsql;


----------------------------------------------------------------
-- Computa a próxima combinação                               --
-- n tem que ser maior ou igual que k para não ter repetição  --
----------------------------------------------------------------

CREATE OR REPLACE FUNCTION kdd.util_combinacoes_proxima(
  combinacao integer[], n integer, k integer
) RETURNS integer[] AS $$
DECLARE
  u integer;
BEGIN
  -- primeira combinação
  IF array_length(combinacao, 1) IS NULL THEN
    FOR i IN 1..k LOOP
        combinacao[i] := i;
    END LOOP;
	RETURN combinacao;
  END IF;
  
  -- onde termina de estourar?
  u := 0;
  FOR i IN reverse k..1 LOOP
    IF combinacao[i] < n - (k - i) THEN
      u := i;
      EXIT; -- não estourou, existe possibilidade na posição u
    END IF;
  END LOOP;
  
  IF u = 0 THEN
    RAISE NOTICE 'não existem mais combinações';
	RETURN combinacao;
  ELSE
    -- raise notice 'existe possibilidade em %', u;
  END IF;
  
  -- ir para a próxima possibilidade em u
  combinacao[u] := combinacao[u] + 1;
  
  -- gerar próxima combinação
  FOR i IN (u+1)..k LOOP
    combinacao[i] := combinacao[i-1] + 1;
  END LOOP;

  --
  RETURN combinacao;
END;
$$ LANGUAGE plpgsql;


------------------------
-- Mineração de texto --
------------------------

CREATE OR REPLACE FUNCTION kdd.util_mineracao_texto_tfidf(
  p_id_processo integer, p_id_atributo integer, p_max_ranking integer, p_min_tfidf numeric
) RETURNS TABLE (id_texto integer, hash integer, palavra character varying) AS
$$
  WITH palavras_documentos AS (
    SELECT
        d.id_texto AS doc_id,
		x.a AS palavra
    FROM 
	  (SELECT id_texto, texto FROM kdd.textos WHERE id_processo = p_id_processo AND id_atributo = p_id_atributo) AS d, 
	  UNNEST(to_tsvector('portuguese', d.texto)) AS x(a,b,c)
	),
	contagem_palavras_doc AS (
	    SELECT
	        doc_id,
	        palavra,
	        COUNT(*) AS ocorrencias
	    FROM palavras_documentos
	    WHERE palavra <> ''
	    GROUP BY doc_id, palavra
	),
	total_palavras_doc AS (
	    SELECT
	        doc_id,
	        COUNT(*) AS total_palavras
	    FROM palavras_documentos
	    WHERE palavra <> ''
	    GROUP BY doc_id
	),
	tf AS (
	    SELECT
	        c.doc_id,
	        c.palavra,
	        c.ocorrencias::FLOAT / t.total_palavras AS tf
	    FROM contagem_palavras_doc c
	    JOIN total_palavras_doc t ON c.doc_id = t.doc_id
	),
	docs_com_palavra AS (
	    SELECT
	        palavra,
	        COUNT(DISTINCT doc_id) AS docs_com_essa_palavra
	    FROM contagem_palavras_doc
	    GROUP BY palavra
	),
	total_docs AS (
	    SELECT COUNT(*) AS total_docs 
		FROM kdd.textos WHERE id_processo = p_id_processo AND id_atributo = p_id_atributo
	),
	idf AS (
	    SELECT
	        d.palavra,
	        LOG( (td.total_docs::FLOAT) / (d.docs_com_essa_palavra) ) AS idf
	    FROM docs_com_palavra d, total_docs td
	)
	SELECT doc_id AS id_texto, kdd.util_hash_text(palavra || palavra) AS hash, palavra FROM (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY tf DESC, idf DESC) AS ranking
		FROM (
			SELECT tf.doc_id, tf.palavra, tf.tf, idf.idf, ROUND(tf.tf * idf.idf * 1000000) / 100000 AS tfidf
			FROM tf JOIN idf ON tf.palavra = idf.palavra
		) AS a) AS b
	WHERE ranking <= p_max_ranking AND tfidf >= p_min_tfidf;		
$$
LANGUAGE 'sql' IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION kdd.util_mineracao_texto(
  p_id_processo integer, p_max_ranking integer, p_min_tfidf numeric, 
  p_id_atributo integer
) RETURNS void AS $$
BEGIN

  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome)
    SELECT DISTINCT p_id_processo, p_id_atributo, hash, palavra 
	FROM kdd.util_mineracao_texto_tfidf(p_id_processo, p_id_atributo, p_max_ranking, p_min_tfidf);

  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item) 
	SELECT p_id_processo, id_texto, p_id_atributo, hash
	FROM kdd.util_mineracao_texto_tfidf(p_id_processo, p_id_atributo, p_max_ranking, p_min_tfidf);
  
END;
$$ LANGUAGE plpgsql;


---------------------------------------------
--  Inicializa o ambiente com um processo  --
---------------------------------------------

INSERT INTO kdd.processos (id_processo, titulo, descricao)
  VALUES (1, 'Processo Teste', 'Processo de KDD utilizado para testes do Agadê Mineração')
  ON CONFLICT DO NOTHING;
