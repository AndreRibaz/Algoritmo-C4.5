---------------------------------------
--  Exemplo 2 do classificador C4.5  --
---------------------------------------

/*
  INSERT INTO kdd.processos (id_processo, titulo, descricao)
    VALUES (10, 'Exemplo 2 do classificador C4.5', 'Exemplo 2 do classificador C4.5')
    ON CONFLICT DO NOTHING;

  CREATE TABLE IF NOT EXISTS kdd.c45_bilheteria
  (
    id integer NOT NULL,
    titulo_original character varying(100) NOT NULL,
    titulo_brasileiro character varying(100) NOT NULL,
    genero character varying(100) NOT NULL,
    pais_origem character varying(100) NOT NULL,
    cpb_roe character varying(100) NOT NULL,
    copias integer NOT NULL,
    salas integer NOT NULL,
    dt_inicio_exibicao date NOT NULL,
    dt_fim_exibicao date NOT NULL,
    publico integer NOT NULL,
    renda numeric NOT NULL,
    razao_social_distribuidora character varying(100) NOT NULL,
    registro_distribuidora integer NOT NULL,
    cnpj_distribuidora character varying(100) NOT NULL,
    ano_cinematografico date,
    CONSTRAINT pk_bilheteria PRIMARY KEY (id)
  );

  -- importar dados do csv pelo pgAdmin4 para a tabela kdd.c45_bilheteria

  DROP TABLE c45_bilheteria;
*/

CREATE OR REPLACE PROCEDURE kdd.processo_kdd_10()
AS $$
DECLARE
  v_id_processo integer := 10;
BEGIN

  -------------------------
  --  Pré-processamento  --
  -------------------------

  -- inicia o processo de KDD
  PERFORM kdd.util_iniciar_processo_kdd(v_id_processo);

  -- atributos
  INSERT INTO kdd.atributos (id_processo, id_atributo, nome, tipo) VALUES
    (v_id_processo,  3, 'genero',      'C'),
    (v_id_processo,  4, 'pais_origem', 'C'),
	(v_id_processo, 10, 'publico',     'N'),
	(v_id_processo, 11, 'renda',       'N');

  -- itens
  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome) 
    SELECT DISTINCT v_id_processo, 3, kdd.util_hash_text(genero), genero
    FROM kdd.c45_bilheteria;

  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome) 
    SELECT DISTINCT v_id_processo, 4, kdd.util_hash_text(pais_origem), pais_origem
    FROM kdd.c45_bilheteria;

  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome) 
    SELECT DISTINCT v_id_processo, 10, kdd.util_hash_text(publico || ' '), publico || ' '
    FROM kdd.c45_bilheteria;

  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome) 
    SELECT DISTINCT v_id_processo, 11, kdd.util_hash_text(renda || ' '), renda || ' '
    FROM kdd.c45_bilheteria;

  /*
  -- titulo_original
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 1, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 1 AND i.nome = e.titulo_original;

  -- titulo_brasileiro
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 2, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 2 AND i.nome = e.titulo_brasileiro;
  */
  
  -- genero
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 3, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 3 AND i.nome = e.genero;

  -- pais_origem
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 4, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 4 AND i.nome = e.pais_origem;

  /*
  -- cpb_roe
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 5, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 5 AND i.nome = e.cpb_roe;

  -- copias
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 7, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 7 AND i.nome = e.copias::text;

  -- salas
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 7, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 7 AND i.nome = e.salas::text
    ON CONFLICT (id_processo, id_transacao, id_atributo, id_item) DO NOTHING;

  -- dt_inicio_exibicao
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 8, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 8 AND i.nome = e.dt_inicio_exibicao::text;

  -- dt_fim_exibicao
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 9, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 9 AND i.nome = e.dt_fim_exibicao::text;
  */

  -- publico
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 10, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 10 AND i.nome = e.publico || ' ';

  -- renda
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 11, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 11 AND i.nome = e.renda || ' ';

  /*
  -- razao_social_distribuidora
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 12, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 12 AND i.nome = e.razao_social_distribuidora;

  -- registro_distribuidora
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 13, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 13 AND i.nome = e.registro_distribuidora::text;

  -- cnpj_distribuidora
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 14, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 14 AND i.nome = e.cnpj_distribuidora;

  -- ano_cinematografico
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 15, i.id_item
    FROM kdd.c45_bilheteria e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 15 AND i.nome = e.ano_cinematografico::text;
  */


  --------------------------
  --  Mineração de dados  --
  -------------------------- 

  PERFORM kdd.c45(
    v_id_processo,                                  -- id_processo
    'genero',                                       -- atributo-alvo
    ARRAY['pais_origem', 'renda', 'publico'],       -- atributos-preditores
	true                                            -- computar a árvore de decisão
  );


  -------------------------
  --  Pós-processamento  --
  -------------------------

  -- inicia o pós-processamento
  PERFORM kdd.util_iniciar_pos_processamento();

  -- finaliza o processo de KDD
  PERFORM kdd.util_finalizar_processo_kdd(v_id_processo);
	
END;
$$ LANGUAGE plpgsql;


--------------------------------
--  Inicia o processo de KDD  --
--------------------------------

DO $$
BEGIN
  -- computa e visualiza a árvore de decisão
  CALL kdd.processo_kdd_10();

  /*
  -- apenas visualiza a árvore de decisão
  PERFORM kdd.c45(
    10,                                             -- id_processo
    'genero',                                       -- atributo-alvo
    ARRAY['pais_origem', 'renda', 'publico'],       -- atributos-preditores
	false                                           -- computar a árvore de decisão
  );
  */
END $$;

/*
SELECT * 
FROM kdd.atributos
WHERE id_processo = 10
ORDER BY id_atributo;

SELECT * 
FROM kdd.itens
WHERE id_processo = 10
ORDER BY id_atributo, id_item;

SELECT id_transacao, id_atributo, id_item
FROM kdd.transacoes
WHERE id_processo = 10
ORDER BY id_transacao, id_atributo, id_item;
*/

