--------------------------
--  Classificador C4.5  --
--------------------------

CREATE OR REPLACE FUNCTION kdd.c45_construir_arvore_decisao(
    p_id_processo integer,
    p_target_attribute integer,
    p_predictor_attributes integer[],
    p_preditores_tipos character[],
    p_min_gain_limit float DEFAULT 0.2, -- força maior seletividade na escolha do atributo
    p_min_samples_div integer DEFAULT 1,
    p_max_prof integer DEFAULT 2, -- edita a profundidade da árvore
    p_current_prof integer DEFAULT 0,
    p_parent_node_id integer DEFAULT NULL,
    p_parent_value text DEFAULT NULL,
    p_session_suffix text DEFAULT NULL,
    p_parent_transactions_table text DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_node_id integer;
    v_total_count integer;
    v_entropy float;
    v_best_attribute integer;
    v_best_gain float := -1;
    v_best_split text;
    v_is_numeric boolean;
    v_majority_class text;
    v_majority_count integer;
    v_current_attribute integer;
    v_current_gain float;
    v_current_split text;
    i integer;
    v_attr_type character; -- MOVIDO PARA O ESCOPO PRINCIPAL
    v_current_transactions_table text;
    v_transactions_source_table text;
    v_parent_split_attr integer; -- MOVIDO PARA O ESCOPO PRINCIPAL
BEGIN
    -- depuração
	-- RAISE NOTICE 'DEPURANDO: %, %', p_parent_node_id, p_parent_value;

    -- Gerar um sufixo de sessão se não foi fornecido
    IF p_session_suffix IS NULL THEN
        p_session_suffix := floor(random() * 1000000)::text;
    END IF;

    -- Nome da tabela temporária para as transações deste nó
    v_current_transactions_table := 'temp_transacoes_' || p_session_suffix || '_' || COALESCE(p_parent_node_id::text, 'root');

    -- Se é o nó raiz, criar tabela temporária com todas as transações do processo
    IF p_parent_node_id IS NULL THEN
        EXECUTE format('
            CREATE TEMP TABLE IF NOT EXISTS %I AS
            SELECT DISTINCT id_transacao
            FROM kdd.transacoes
            WHERE id_processo = %s',
            v_current_transactions_table,
            p_id_processo
        );
        v_transactions_source_table := v_current_transactions_table;
    ELSE
        -- Para nós não raiz, filtrar da tabela temporária do nó pai
        v_transactions_source_table := p_parent_transactions_table;

        -- Atribuição de v_parent_split_attr e v_attr_type para o escopo principal
        v_parent_split_attr := (SELECT split_attribute::integer FROM kdd.arvore_decisao WHERE node_id = p_parent_node_id AND id_processo = p_id_processo);
        v_attr_type := p_preditores_tipos[array_position(p_predictor_attributes, v_parent_split_attr)];

        IF v_attr_type = 'N' THEN
            -- Para atributos numéricos
            EXECUTE format('
                CREATE TEMP TABLE IF NOT EXISTS %I AS
                SELECT DISTINCT t.id_transacao
                FROM %I t
                JOIN kdd.transacoes full_t ON full_t.id_transacao = t.id_transacao AND full_t.id_processo = %s AND full_t.id_atributo = %s
                JOIN kdd.itens i ON i.id_item = full_t.id_item AND i.id_processo = full_t.id_processo AND i.id_atributo = full_t.id_atributo
                WHERE full_t.id_processo = %s AND full_t.id_atributo = %s
                AND CASE
                    WHEN i.nome ~ ''^\d+(\.\d+)?$'' THEN i.nome::numeric %s
                    ELSE FALSE
                END
            ',
                v_current_transactions_table,
                v_transactions_source_table,
                p_id_processo,
                v_parent_split_attr,
                p_id_processo,
                v_parent_split_attr,
                CASE
                    WHEN p_parent_value LIKE '≤ %' THEN '<= ' || split_part(p_parent_value, ' ', 2)
                    WHEN p_parent_value LIKE '> %' THEN '> ' || split_part(p_parent_value, ' ', 2)
                    ELSE '= ' || split_part(p_parent_value, ' ', 2)
                END
            );
        ELSE -- v_attr_type is not 'N' (categorical)
            -- Para atributos categóricos
            EXECUTE format('
                CREATE TEMP TABLE IF NOT EXISTS %I AS
                SELECT DISTINCT t.id_transacao
                FROM %I t
                JOIN kdd.transacoes full_t ON full_t.id_transacao = t.id_transacao AND full_t.id_processo = %s AND full_t.id_atributo = %s
                JOIN kdd.itens i ON i.id_item = full_t.id_item AND i.id_processo = full_t.id_processo AND i.id_atributo = full_t.id_atributo
                WHERE full_t.id_processo = %s AND full_t.id_atributo = %s
                AND i.nome = %L
            ',
                v_current_transactions_table,
                v_transactions_source_table,
                p_id_processo,
                v_parent_split_attr,
                p_id_processo,
                v_parent_split_attr,
                p_parent_value
            );
        END IF;
    END IF;

    -- Total de amostras
    EXECUTE format('
        SELECT COUNT(DISTINCT t.id_transacao)
        FROM kdd.transacoes t
        JOIN %I tmp ON t.id_transacao = tmp.id_transacao
        WHERE t.id_processo = %s AND t.id_atributo = %s',
        v_current_transactions_table,
        p_id_processo, p_target_attribute
    ) INTO v_total_count;

    -- Se não há amostras suficientes, retornar
    IF v_total_count < p_min_samples_div THEN
        -- Limpar tabela temporária
        EXECUTE format('DROP TABLE IF EXISTS %I', v_current_transactions_table);
        RETURN;
    END IF;

    -- Entropia
    EXECUTE format('
        SELECT SUM(-(count::float / total) *
               CASE WHEN count = 0 THEN 0 ELSE LN(count::float / total) / LN(2) END)
        FROM (
            SELECT COUNT(*) AS count
            FROM kdd.transacoes t
            JOIN %I tmp ON t.id_transacao = tmp.id_transacao
            WHERE t.id_processo = %s AND t.id_atributo = %s
            GROUP BY t.id_item
        ) AS sub,
        (SELECT COUNT(*) AS total
         FROM kdd.transacoes t
         JOIN %I tmp ON t.id_transacao = tmp.id_transacao
         WHERE t.id_processo = %s AND t.id_atributo = %s) AS total',
        v_current_transactions_table, p_id_processo, p_target_attribute,
        v_current_transactions_table, p_id_processo, p_target_attribute
    ) INTO v_entropy;

    -- Classe majoritária
    EXECUTE format('
        SELECT i.nome, COUNT(*)
        FROM kdd.transacoes t
        JOIN %I tmp ON t.id_transacao = tmp.id_transacao
        JOIN kdd.itens i ON i.id_item = t.id_item AND i.id_atributo = t.id_atributo AND i.id_processo = t.id_processo
        WHERE t.id_processo = %s AND t.id_atributo = %s
        GROUP BY i.nome
        ORDER BY COUNT(*) DESC
        LIMIT 1',
        v_current_transactions_table, p_id_processo, p_target_attribute
    ) INTO v_majority_class, v_majority_count;

    -- ID do nó
    SELECT COALESCE(MAX(unique_node_id), 0) + 1 INTO v_node_id FROM kdd.arvore_decisao WHERE id_processo = p_id_processo;

    -- Critérios de parada
    IF v_entropy = 0 OR v_total_count < p_min_samples_div OR p_current_prof >= p_max_prof THEN
        INSERT INTO kdd.arvore_decisao VALUES (
            p_id_processo,
            v_node_id, v_node_id, p_parent_node_id, p_parent_value,
            NULL, NULL, true,
            v_majority_class, v_majority_count, v_total_count,
            p_current_prof, 0
        );
        -- Limpar tabela temporária
        EXECUTE format('DROP TABLE IF EXISTS %I', v_current_transactions_table);
        RETURN;
    END IF;

    -- Loop por atributos preditores
    FOR i IN 1 .. array_length(p_predictor_attributes, 1) LOOP
        v_current_attribute := p_predictor_attributes[i];
        SELECT tipo INTO v_attr_type
        FROM kdd.atributos
        WHERE id_processo = p_id_processo AND id_atributo = v_current_attribute;

        IF v_attr_type = 'N' THEN
            -- Atributo numérico: calcular mediana como ponto de divisão
            EXECUTE format('
                SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY i.nome::numeric)
                FROM kdd.transacoes t
                JOIN %I tmp ON t.id_transacao = tmp.id_transacao
                JOIN kdd.itens i ON i.id_item = t.id_item AND i.id_processo = t.id_processo AND i.id_atributo = t.id_atributo
                WHERE t.id_processo = %s AND t.id_atributo = %s
                      AND i.nome ~ ''^\d+(\.\d+)?$''',
                v_current_transactions_table, p_id_processo, v_current_attribute
            ) INTO v_current_split;

            -- Calcular ganho de informação para divisão numérica
            EXECUTE format('
                WITH dados AS (
                    SELECT
                        i.nome::numeric <= %L AS lado,
                        classe.nome AS classe
                    FROM kdd.transacoes t
                    JOIN %I tmp ON t.id_transacao = tmp.id_transacao
                    JOIN kdd.itens i ON i.id_item = t.id_item AND i.id_processo = t.id_processo AND i.id_atributo = t.id_atributo
                    JOIN kdd.transacoes t_classe ON t_classe.id_transacao = t.id_transacao
                        AND t_classe.id_processo = t.id_processo AND t_classe.id_atributo = %s
                    JOIN kdd.itens classe ON classe.id_item = t_classe.id_item AND classe.id_atributo = %s AND classe.id_processo = %s
                    WHERE t.id_processo = %s AND t.id_atributo = %s AND i.nome ~ ''^\d+(\.\d+)?$''
                ),
                calc AS (
                    SELECT lado, classe, COUNT(*)::float AS cnt
                    FROM dados GROUP BY lado, classe
                ),
                total_lado AS (
                    SELECT lado, SUM(cnt) AS total FROM calc GROUP BY lado
                ),
                entropia AS (
                    SELECT c.lado, t.total,
                           SUM(-(c.cnt/t.total) * CASE WHEN c.cnt=0 THEN 0 ELSE LN(c.cnt/t.total)/LN(2) END) AS e
                    FROM calc c JOIN total_lado t ON c.lado = t.lado
                    GROUP BY c.lado, t.total
                )
                SELECT %s - SUM((total / %s) * e) FROM entropia',
                v_current_split, v_current_transactions_table, p_target_attribute, p_target_attribute, p_id_processo,
                p_id_processo, v_current_attribute, v_entropy, v_total_count
            ) INTO v_current_gain;
        ELSE -- v_attr_type is not 'N' (categorical)
            -- Atributo categórico
            v_current_split := NULL;
            -- Calcular ganho de informação para atributo categórico
            EXECUTE format('
                WITH dados AS (
                    SELECT
                        i.nome AS valor,
                        classe.nome AS classe
                    FROM kdd.transacoes t
                    JOIN %I tmp ON t.id_transacao = tmp.id_transacao
                    JOIN kdd.itens i ON i.id_item = t.id_item AND i.id_processo = t.id_processo AND i.id_atributo = t.id_atributo
                    JOIN kdd.transacoes t_classe ON t_classe.id_transacao = t.id_transacao
                        AND t_classe.id_processo = t.id_processo AND t_classe.id_atributo = %s
                    JOIN kdd.itens classe ON classe.id_item = t_classe.id_item AND classe.id_atributo = %s AND classe.id_processo = %s
                    WHERE t.id_processo = %s AND t.id_atributo = %s
                ),
                calc AS (
                    SELECT valor, classe, COUNT(*)::float AS cnt
                    FROM dados GROUP BY valor, classe
                ),
                total_valor AS (
                    SELECT valor, SUM(cnt) AS total FROM calc GROUP BY valor
                ),
                entropia AS (
                    SELECT c.valor, t.total,
                           SUM(-(c.cnt/t.total) * CASE WHEN c.cnt=0 THEN 0 ELSE LN(c.cnt/t.total)/LN(2) END) AS e
                    FROM calc c JOIN total_valor t ON c.valor = t.valor
                    GROUP BY c.valor, t.total
                )
                SELECT %s - SUM((total / %s) * e) FROM entropia',
                v_current_transactions_table, p_target_attribute, p_target_attribute, p_id_processo,
                p_id_processo, v_current_attribute, v_entropy, v_total_count
            ) INTO v_current_gain;
        END IF;

        IF v_current_gain > v_best_gain THEN
            v_best_gain := v_current_gain;
            v_best_attribute := v_current_attribute;
            v_best_split := v_current_split;
        END IF;
    END LOOP;

    -- Parada por ganho insuficiente
    IF v_best_gain < p_min_gain_limit THEN
        INSERT INTO kdd.arvore_decisao VALUES (
            p_id_processo,
            v_node_id, v_node_id, p_parent_node_id, p_parent_value,
            NULL, NULL, true,
            v_majority_class, v_majority_count, v_total_count,
            p_current_prof, 0
        );
        -- Limpar tabela temporária
        EXECUTE format('DROP TABLE IF EXISTS %I', v_current_transactions_table);
        RETURN;
    END IF;

    -- Inserção do nó de decisão
    INSERT INTO kdd.arvore_decisao VALUES (
        p_id_processo,
        v_node_id, v_node_id, p_parent_node_id, p_parent_value,
        v_best_attribute::text, v_best_split, false,
        v_majority_class, v_majority_count, v_total_count,
        p_current_prof, v_best_gain
    );

    -- Ramificação baseada no tipo de atributo
    IF v_best_split IS NOT NULL THEN
        -- Atributo numérico: criar ramos ≤ e >

        -- Ramo esquerdo (≤ split)
        PERFORM kdd.c45_construir_arvore_decisao(
            p_id_processo,
            p_target_attribute,
            p_predictor_attributes,
            p_preditores_tipos,
            p_min_gain_limit,
            p_min_samples_div,
            p_max_prof,
            p_current_prof + 1,
            v_node_id,
            '≤ ' || v_best_split,
            p_session_suffix,
            v_current_transactions_table
        );

        -- Ramo direito (> split)
        PERFORM kdd.c45_construir_arvore_decisao(
            p_id_processo,
            p_target_attribute,
            p_predictor_attributes,
            p_preditores_tipos,
            p_min_gain_limit,
            p_min_samples_div,
            p_max_prof,
            p_current_prof + 1,
            v_node_id,
            '> ' || v_best_split,
            p_session_suffix,
            v_current_transactions_table
        );
    ELSE
        -- Atributo categórico: ramificar para cada valor possível
        FOR v_current_split IN
            EXECUTE format('
                SELECT DISTINCT i.nome
                FROM kdd.transacoes t
                JOIN %I tmp ON t.id_transacao = tmp.id_transacao
                JOIN kdd.itens i ON i.id_item = t.id_item AND i.id_processo = t.id_processo AND i.id_atributo = t.id_atributo
                WHERE t.id_processo = %s AND t.id_atributo = %s
                  AND i.nome IS NOT NULL',
                v_current_transactions_table, p_id_processo, v_best_attribute)
        LOOP
            -- Recursão para cada valor do atributo categórico
            PERFORM kdd.c45_construir_arvore_decisao(
                p_id_processo,
                p_target_attribute,
                p_predictor_attributes,
                p_preditores_tipos,
                p_min_gain_limit,
                p_min_samples_div,
                p_max_prof,
                p_current_prof + 1,
                v_node_id,
                v_current_split,
                p_session_suffix,
                v_current_transactions_table
            );
        END LOOP;
    END IF;

    -- Limpar tabela temporária após uso
    EXECUTE format('DROP TABLE IF EXISTS %I', v_current_transactions_table);
END;
$$;


-------------------------
--  Árvore de Decisão  --
-------------------------

CREATE OR REPLACE FUNCTION kdd.c45(
    p_id_processo integer,
    p_alvo text,
    p_preditores text[],
	p_computar_arvore_decisao boolean = true
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_target_attribute integer;
    v_predictor_attributes integer[];
    v_predictor_types character[];
    i integer;
    v_attr_id integer;
    v_attr_type character;
    total_registros integer;
    entropia_inicial float;
    contador integer := 0;
    total_nos integer := 0;
    registro record;
    identacao text;
    linha text;
    v_nome_atributo text;
    v_nome_item text;
	v_tipo_atributo character(1);
BEGIN
    -- Buscar ID e tipo do atributo alvo
    SELECT id_atributo, tipo INTO v_target_attribute, v_attr_type
    FROM kdd.atributos
    WHERE nome = p_alvo AND id_processo = p_id_processo
    LIMIT 1;

    IF v_target_attribute IS NULL THEN
        RAISE EXCEPTION 'Atributo alvo "%" não encontrado no processo %', p_alvo, p_id_processo;
    END IF;

    -- Buscar IDs e tipos dos atributos preditores
    v_predictor_attributes := '{}'::integer[];
    v_predictor_types := '{}'::character[];
    
    FOR i IN 1 .. array_length(p_preditores, 1) LOOP
        SELECT id_atributo, tipo INTO v_attr_id, v_attr_type
        FROM kdd.atributos
        WHERE nome = p_preditores[i] AND id_processo = p_id_processo
        LIMIT 1;

        IF v_attr_id IS NULL THEN
            RAISE EXCEPTION 'Atributo preditor "%" não encontrado no processo %', p_preditores[i], p_id_processo;
        END IF;

        v_predictor_attributes := array_append(v_predictor_attributes, v_attr_id);
        v_predictor_types := array_append(v_predictor_types, v_attr_type);
    END LOOP;

    -- Verificar se existem dados para processar
    IF NOT EXISTS (
        SELECT 1 FROM kdd.transacoes 
        WHERE id_processo = p_id_processo 
        AND id_atributo = v_target_attribute
    ) THEN
        RAISE EXCEPTION 'Nenhum dado encontrado para o atributo alvo "%" no processo %', p_alvo, p_id_processo;
    END IF;

    -- Mostrar cabeçalho
    RAISE NOTICE 'ALGORITMO C4.5 (beta)';
    RAISE NOTICE '---------------------';
	RAISE NOTICE '';

    -- Chamar construção da árvore com os tipos obtidos da tabela
	IF p_computar_arvore_decisao THEN
	    PERFORM kdd.c45_construir_arvore_decisao(
	        p_id_processo,
	        v_target_attribute,
	        v_predictor_attributes,
	        v_predictor_types,
	        0.2, 1, 2, 0, NULL::integer, NULL,
	        FLOOR(random() * 1000000)::text,
			NULL::text
	    );
	END IF;

    SELECT COUNT(*) INTO total_nos FROM kdd.arvore_decisao
	WHERE id_processo = p_id_processo;
    contador := 0;
	v_nome_atributo := '';
	identacao := '';
	
    FOR registro IN
        SELECT unique_node_id, parent_value, is_leaf, split_attribute, split_value,
          class_value, class_count, total_count, node_depth AS nivel_no, information_gain
        FROM kdd.arvore_decisao
		WHERE id_processo = p_id_processo
        ORDER BY unique_node_id
    LOOP
		-- Contrução das linhas das árvores
        contador := contador + 1;
       	IF contador > 1 THEN 
			identacao := repeat('│   ', registro.nivel_no - 1);

	        IF contador = total_nos THEN
	            identacao := identacao || '└── ';
	        ELSE
	            identacao := identacao || '├── ';
	        END IF;
		END IF;

		-- Buscar nome e tipo do atributo
		IF contador > 1 THEN
        	SELECT nome, tipo INTO v_nome_atributo, v_tipo_atributo FROM kdd.atributos
        	WHERE id_processo = p_id_processo
			AND id_atributo = (SELECT split_attribute FROM kdd.arvore_decisao 
			                   WHERE id_processo = p_id_processo
						       AND unique_node_id = (SELECT parent_node_id FROM kdd.arvore_decisao
			                                         WHERE id_processo = p_id_processo
											         AND unique_node_id = registro.unique_node_id))::integer;
		END IF;

        -- Determinar valor (item) com nome real
        IF registro.split_value IS NOT NULL AND registro.split_value ~ '^[0-9]+(\\.[0-9]+)?$' THEN
            v_nome_item := registro.split_value;
        ELSE
            SELECT nome INTO v_nome_item
            FROM kdd.itens
            WHERE id_processo = p_id_processo AND id_atributo = registro.split_attribute::integer
              AND id_item::text = registro.split_value;
        END IF;

  		--- Imprime nó da árvore
		IF contador = 1 THEN -- raiz
			linha := 'Árvore de decisão';
		ELSE
			IF v_tipo_atributo = 'C' THEN
				linha := identacao || v_nome_atributo || ' = ' || registro.parent_value;
			ELSE
				linha := identacao || v_nome_atributo || ' ' || registro.parent_value;
			END IF;
		END IF;

		IF registro.is_leaf THEN
			linha := linha || ': ' || p_alvo || ' = ' || registro.class_value || ' (' || registro.class_count || '/' || registro.total_count || ')';
		ELSE
			linha := linha || ' (Ganho: ' || ROUND(registro.information_gain::numeric, 3) || ')';
		END IF;
		RAISE NOTICE '%', linha;
		
    END LOOP;

    RAISE NOTICE '-----------------------------';
END;
$$;

