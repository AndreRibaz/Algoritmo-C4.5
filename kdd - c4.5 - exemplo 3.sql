---------------------------------------
--  Exemplo 2 do classificador C4.5  --
---------------------------------------


/*
CREATE TABLE IF NOT EXISTS kdd.inep_ideb_uf (
    id serial NOT NULL,
    ano INT NOT NULL,
    sigla_uf character varying(2) NOT NULL,
    rede character varying(50) NOT NULL,
    ensino character varying(50) NOT NULL,
    anos_escolares character varying(50) NOT NULL,
    taxa_aprovacao numeric NOT NULL,
    indicador_rendimento numeric NOT NULL,
    nota_saeb_matematica numeric NOT NULL,
    nota_saeb_lingua_portuguesa numeric NOT NULL,
    nota_saeb_media_padronizada numeric NOT NULL,
    ideb numeric NOT NULL,
    CONSTRAINT pk_ideb_uf PRIMARY KEY (id)
);

COMMENT ON TABLE kdd.inep_ideb_uf IS 'Tabela de indicadores educacionais do Brasil (2005-2023)';
COMMENT ON COLUMN kdd.inep_ideb_uf.ano IS 'Ano do dado (2005-2023)';
COMMENT ON COLUMN kdd.inep_ideb_uf.sigla_uf IS 'Sigla da Unidade da Federação';
COMMENT ON COLUMN kdd.inep_ideb_uf.rede IS 'Rede Escolar';
COMMENT ON COLUMN kdd.inep_ideb_uf.ensino IS 'Tipo de Ensino';
COMMENT ON COLUMN kdd.inep_ideb_uf.anos_escolares IS 'Anos Escolares';
COMMENT ON COLUMN kdd.inep_ideb_uf.taxa_aprovacao IS 'Taxa de Aprovação';
COMMENT ON COLUMN kdd.inep_ideb_uf.indicador_rendimento IS 'Indicador de Rendimento (P)';
COMMENT ON COLUMN kdd.inep_ideb_uf.nota_saeb_matematica IS 'Nota SAEB - Matemática';
COMMENT ON COLUMN kdd.inep_ideb_uf.nota_saeb_lingua_portuguesa IS 'Nota SAEB - Língua Portuguesa';
COMMENT ON COLUMN kdd.inep_ideb_uf.nota_saeb_media_padronizada IS 'Nota SAEB - Média Padronizada (N)';
COMMENT ON COLUMN kdd.inep_ideb_uf.ideb IS 'IDEB (N x P)';
*/


INSERT INTO kdd.processos (id_processo, titulo, descricao)
  VALUES (7, 'Exemplo 2 do Classificador C4.5', 'Exemplo 2 do Classificador C4.5')
  ON CONFLICT DO NOTHING;

CREATE OR REPLACE PROCEDURE kdd.processo_kdd_7()
AS $$
DECLARE
  v_id_processo integer := 7;
  v_count integer;
BEGIN
  -------------------------
  --  Pré-processamento  --
  -------------------------

  -- Verifica se há dados antes de iniciar o processo
  SELECT COUNT(*) INTO v_count FROM kdd.inep_ideb_uf;
  
  IF v_count = 0 THEN
    RAISE EXCEPTION 'Nenhum dado encontrado na tabela kdd.inep_ideb_uf';
  END IF;

  -- inicia o processo de KDD
  PERFORM kdd.util_iniciar_processo_kdd(v_id_processo);

  -- atributos (corrigindo os IDs para serem consistentes)
  INSERT INTO kdd.atributos (id_processo, id_atributo, nome, tipo) VALUES
    (v_id_processo, 1, 'ano', 'N'),
    (v_id_processo, 2, 'sigla_uf', 'C'),
    (v_id_processo, 3, 'rede', 'C'),
    (v_id_processo, 4, 'ensino', 'C'),
    (v_id_processo, 5, 'anos_escolares', 'C'),
    (v_id_processo, 6, 'taxa_aprovacao', 'N'),
    (v_id_processo, 7, 'indicador_rendimento', 'N'),
    (v_id_processo, 8, 'nota_saeb_matematica', 'N'),
    (v_id_processo, 9, 'nota_saeb_lingua_portuguesa', 'N'),
    (v_id_processo, 10, 'nota_saeb_media_padronizada', 'N'),
    (v_id_processo, 11, 'ideb', 'N');


  -- itens
  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome) 
    SELECT DISTINCT v_id_processo, 2, kdd.util_hash_text(sigla_uf), sigla_uf
    FROM kdd.inep_ideb_uf;

  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome) 
    SELECT DISTINCT v_id_processo, 3, kdd.util_hash_text(rede), rede
    FROM kdd.inep_ideb_uf;

  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome) 
    SELECT DISTINCT v_id_processo, 4, kdd.util_hash_text(ensino), ensino
    FROM kdd.inep_ideb_uf;

  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome) 
    SELECT DISTINCT v_id_processo, 6, kdd.util_hash_text(taxa_aprovacao || ' '), taxa_aprovacao || ' '
    FROM kdd.inep_ideb_uf;

  INSERT INTO kdd.itens (id_processo, id_atributo, id_item, nome) 
    SELECT DISTINCT v_id_processo, 11, kdd.util_hash_text(ideb || ' '), ideb || ' '
    FROM kdd.inep_ideb_uf;


  -- transações
/*
  -- ano
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 1, i.id_item
    FROM kdd.inep_ideb_uf e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 1 AND i.nome = e.ano;
*/

  -- sigla_uf
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 2, i.id_item
    FROM kdd.inep_ideb_uf e 
    INNER JOIN kdd.itens i ON i.id_processo = v_id_processo AND i.id_atributo = 2 AND i.nome = e.sigla_uf
    WHERE e.sigla_uf IS NOT NULL;
  
  -- rede
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 3, i.id_item
    FROM kdd.inep_ideb_uf e 
    INNER JOIN kdd.itens i ON i.id_processo = v_id_processo AND i.id_atributo = 3 AND i.nome = e.rede
    WHERE e.rede IS NOT NULL;

  -- ensino
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 4, i.id_item
    FROM kdd.inep_ideb_uf e 
    INNER JOIN kdd.itens i ON i.id_processo = v_id_processo AND i.id_atributo = 4 AND i.nome = e.ensino
    WHERE e.ensino IS NOT NULL;

  -- taxa_aprovacao (corrigindo o id_atributo para 6 conforme definido acima)
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 6, i.id_item
    FROM kdd.inep_ideb_uf e 
    INNER JOIN kdd.itens i ON i.id_processo = v_id_processo AND i.id_atributo = 6 AND i.nome = e.taxa_aprovacao || ' '
    WHERE e.taxa_aprovacao IS NOT NULL;

  -- ideb
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 11, i.id_item
    FROM kdd.inep_ideb_uf e 
    INNER JOIN kdd.itens i ON i.id_processo = v_id_processo AND i.id_atributo = 11 AND i.nome = e.ideb || ' '
    WHERE e.ideb IS NOT NULL;

 /*
  -- anos_escolares
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 5, i.id_item
    FROM kdd.inep_ideb_uf e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 5 AND i.nome = e.anos_escolares;

  -- indicado_rendimento
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 7, i.id_item
    FROM kdd.inep_ideb_uf e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 7 AND i.nome = e.indicado_rendimento::text;

  -- nota_saeb_matematica
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 7, i.id_item
    FROM kdd.inep_ideb_uf e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 7 AND i.nome = e.nota_saeb_matematica::text
    ON CONFLICT (id_processo, id_transacao, id_atributo, id_item) DO NOTHING;

  -- nota_saeb_lingua_portuguesa
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 8, i.id_item
    FROM kdd.inep_ideb_uf e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 8 AND i.nome = e.nota_saeb_lingua_portuguesa::text;

  -- nota_saeb_media_padronizada
  INSERT INTO kdd.transacoes (id_processo, id_transacao, id_atributo, id_item)
    SELECT v_id_processo, e.id, 9, i.id_item
    FROM kdd.inep_ideb_uf e INNER JOIN kdd.itens i 
	ON i.id_processo = v_id_processo AND i.id_atributo = 9 AND i.nome = e.nota_saeb_media_padronizada::text;
  */

  --------------------------
  --  Mineração de dados  --
  -------------------------- 

  PERFORM kdd.c45(
    v_id_processo,                                  -- id_processo
    'ideb',                                         -- atributo-alvo
    ARRAY['ensino', 'rede', 'taxa_aprovacao', 'sigla_uf'], -- atributos-preditores
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
  CALL kdd.processo_kdd_7();

  /*
  -- apenas visualiza a árvore de decisão
  PERFORM kdd.c45(
    7,                                             -- id_processo
    'ideb',                                       -- atributo-alvo
    ARRAY['ensino', 'rede', 'taxa_aprovacao','sigla_uf'],       -- atributos-preditores
	false                                           -- computar a árvore de decisão
  );
  */
END $$;
