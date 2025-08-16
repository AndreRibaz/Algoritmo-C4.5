---------------------------------------
--  Exemplo 1 do classificador C4.5  --
---------------------------------------

/*
INSERT INTO kdd.processos (id_processo, titulo, descricao)
  VALUES (6, 'Exemplo 1 do classificador C4.5', 'Exemplo 1 do classificador C4.5')
  ON CONFLICT DO NOTHING;
*/

CREATE OR REPLACE PROCEDURE kdd.processo_kdd_06()
AS $$
DECLARE
  v_id_processo integer := 6;
BEGIN

  -------------------------
  --  Pré-processamento  --
  -------------------------

  -- inicia o processo de KDD
  PERFORM kdd.util_iniciar_processo_kdd(v_id_processo);

  -- cria exemplo
  CREATE TEMPORARY TABLE IF NOT EXISTS c45_exemplo (
    id integer NOT NULL PRIMARY KEY,
    aparencia character varying(100) NOT NULL,
    temperatura integer NOT NULL,
    umidade integer NOT NULL,
    vento character varying(100) NOT NULL,
    jogar_tenis character varying(100) NOT NULL
  );

  INSERT INTO c45_exemplo (id, aparencia, temperatura, umidade, vento, jogar_tenis) VALUES
    (1, 'Ensolarada', 75, 70, 'Sim', 'Sim'),
    (2, 'Ensolarada', 80, 90, 'Sim', 'Não'),
    (3, 'Ensolarada', 85, 85, 'Não', 'Não'),
    (4, 'Ensolarada', 72, 95, 'Não', 'Não'),
    (5, 'Ensolarada', 69, 70, 'Não', 'Sim'),
    (6, 'Nublada', 72, 90, 'Sim', 'Sim'),
    (7, 'Nublada', 83, 78, 'Não', 'Sim'),
    (8, 'Nublada', 64, 65, 'Sim', 'Sim'),
    (9, 'Nublada', 81, 75, 'Não', 'Sim'),
    (10, 'Chuvosa', 71, 80, 'Sim', 'Não'),
    (11, 'Chuvosa', 65, 70, 'Sim', 'Não'),
    (12, 'Chuvosa', 75, 80, 'Não', 'Sim'),
    (13, 'Chuvosa', 68, 80, 'Não', 'Sim'),
    (14, 'Chuvosa', 70, 96, 'Não', 'Sim');

  -- atributos
  INSERT INTO kdd.atributos (id_processo, id_atributo, nome, tipo) VALUES
    (v_id_processo, 1, 'aparencia', 'C'),
    (v_id_processo, 2, 'temperatura', 'N'),
    (v_id_processo, 3, 'umidade', 'N'),
    (v_id_processo, 4, 'vento', 'C'),
    (v_id_processo, 5, 'jogar_tenis', 'C');

  -- itens
  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome) VALUES
    (v_id_processo, 1, 1, 'Ensolarada'),
    (v_id_processo, 1, 2, 'Nublada'),
    (v_id_processo, 1, 3, 'Chuvosa'),
    (v_id_processo, 4, 1, 'Sim'),
    (v_id_processo, 4, 2, 'Não'),
    (v_id_processo, 5, 1, 'Sim'),
    (v_id_processo, 5, 2, 'Não');
  
  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome)
    SELECT DISTINCT v_id_processo, 2, temperatura, temperatura || ''
    FROM c45_exemplo;
  
  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome)
    SELECT DISTINCT v_id_processo, 3, umidade, umidade || ''
    FROM c45_exemplo;

  -- transações
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, i.id_atributo, i.id_item
    FROM c45_exemplo e INNER JOIN kdd.itens i
    ON i.id_processo = v_id_processo AND i.id_atributo = 1 AND i.nome = e.aparencia;

  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, i.id_atributo, i.id_item
    FROM c45_exemplo e INNER JOIN kdd.itens i
    ON i.id_processo = v_id_processo AND i.id_atributo = 2 AND i.nome = e.temperatura || '';

  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, i.id_atributo, i.id_item
    FROM c45_exemplo e INNER JOIN kdd.itens i
    ON i.id_processo = v_id_processo AND i.id_atributo = 3 AND i.nome = e.umidade || '';

  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, i.id_atributo, i.id_item
    FROM c45_exemplo e INNER JOIN kdd.itens i
    ON i.id_processo = v_id_processo AND i.id_atributo = 4 AND i.nome = e.vento;

  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, i.id_atributo, i.id_item
    FROM c45_exemplo e INNER JOIN kdd.itens i
    ON i.id_processo = v_id_processo AND i.id_atributo = 5 AND i.nome = e.jogar_tenis;

  -- deleta a tabela do exemplo
  DROP TABLE c45_exemplo;
  

  --------------------------
  --  Mineração de dados  --
  -------------------------- 

  /*
  Aparência = Ensolarada: 
  •⁠  ⁠Umidade ≤ 75: Jogar = Sim 
  •⁠  ⁠Umidade > 75: Jogar = Não 
  Aparência = Nublada: 
  •⁠  ⁠Jogar = Sim 
  Aparência = Chuvosa: 
  •⁠  ⁠Vento = Sim: Jogar = Não 
  •⁠  ⁠Vento = Não: Jogar = Sim
  */

  PERFORM kdd.c45(
    v_id_processo,                                            -- id_processo
    'jogar_tenis',                                            -- atributo-alvo
    ARRAY['temperatura', 'umidade', 'vento', 'aparencia'],    -- atributos-preditores
	true                                                      -- construir árvore de decisão
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
  CALL kdd.processo_kdd_06();

  /*
  -- apenas visualiza a árvore de decisão
  PERFORM kdd.c45(
    6,                                                        -- id_processo
    'jogar_tenis',                                            -- atributo-alvo
    ARRAY['temperatura', 'umidade', 'vento', 'aparencia'],    -- atributos-preditores
	false                                                     -- construir árvore de decisão
  );
  */
END $$;

/*
 
SELECT * 
FROM kdd.atributos
WHERE id_processo = 6
ORDER BY id_atributo;

SELECT * 
FROM kdd.itens
WHERE id_processo = 6
ORDER BY id_atributo, id_item;

SELECT id_transacao, id_atributo, id_item
FROM kdd.transacoes
WHERE id_processo = 6
ORDER BY id_transacao, id_atributo, id_item;
*/

