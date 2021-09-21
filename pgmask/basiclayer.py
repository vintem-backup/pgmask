import psycopg2
import psycopg2.extras

class BasicLayer: #TODO: Adicionar Type Annotations

    #TODO:
    # - Criar função para deletar registros
    # - Criar função para deletar tabelas

    def __init__(self, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME):
       
        super().__init__()
        self.DB_USER = DB_USER
        self.DB_PASSWORD = DB_PASSWORD
        self.DB_HOST = DB_HOST
        self.DB_PORT = DB_PORT
        self.DB_NAME = DB_NAME
    
    def create_connection (self):

        try:
            connection = psycopg2.connect(host = self.DB_HOST, database = self.DB_NAME, 
            user = self.DB_USER, password = self.DB_PASSWORD)

            pointer = connection.cursor(cursor_factory = psycopg2.extras.DictCursor)

            return connection, pointer

        except (Exception, psycopg2.Error) as error: #TODO: TRATAR EXCEÇÃO AQUI

            pass

    def sql_command(self, table_name, keys_dict, action, **kwargs):
        
        """Dado um nome de tabela <table_name> e o dicionário <keys_dict> (em que a chave será o nome da coluna e 
        o valor definirá o tipo de dado gravado - vide documentação do PostgreSQL), retorna o comando SQL ade-
        quado para criação da tabela ou salvamento de dados em uma tabela.
        
        
        Keyword arguments:
        =================
        
        table_name  -- Nome da tabela a ser buscada
        keys_dict   -- Dicionário com as chaves (nomes das colunas) e valores (respectivos tipos dos dados das colunas)
        action      -- O tipo de ação desejada com o comando SQL, criar ou salvar.

        *action options*
        ------------
            - create ==> SQL para criar uma tabela
            - save   ==> SQL para salvar em uma tabela
        
        **kwargs:
        =========
        
        pk -- Chave primária
        """
        
        pk = kwargs.get('pk')
        
        key_list = list(keys_dict.keys()); type_list = list(keys_dict.values())
        
        sql = ''; create = bool(action == 'create'); save = bool(action == 'save')
        
        if (create):
            
            sql = 'CREATE TABLE ' + table_name + ' ('
            
            for i in range (len(keys_dict)-1):
                
                entry = key_list[i] + ' ' + type_list[i]
                
                if (pk):
                    
                    if (key_list[i] == pk):
                        
                        entry = key_list[i] + ' ' + type_list[i] + ' PRIMARY KEY'
                            
                sql = sql + entry + ', '

            sql = sql + key_list[len(keys_dict)-1] + ' ' + type_list[len(keys_dict)-1] + ')'
            
        if (save):
            
            format_code = ''
            
            sql = 'INSERT INTO ' + table_name + ' ('
            
            for i in range (len(keys_dict)-1):
                
                sql = sql + key_list[i] + ' ' + ', '
                
                format_code = format_code + '%s, '

            sql = sql + key_list[len(keys_dict)-1] + ' ' + ') VALUES (' + format_code + '%s)'
                            
        return sql


    def create_table(self, table_name, keys_dict, **kwargs):
        
        """Cria a tabela <table_name>, com as colunas <keys_dict.keys() [i]>. Opcionalmente pode-se definir uma das chaves como 
        chave primária, ao invés da chave default atribuida pelo postgres.
        
        
        Keyword arguments:
        =================
        
        table_name  -- Nome da tabela a ser criada
        keys_dict   -- Dicionário com as chaves (nomes das colunas) e valores (respectivos tipos dos dados das colunas)


        **kwargs:
        ========
        
        pk   -- Chave que será atribuida como primária 
        """
        
        pk = kwargs.get('pk')
        
        table_was_created = False

        connection, pointer = self.create_connection()
        
        try:

            sql_create_query = self.sql_command(table_name, keys_dict, 'create')
                    
            if (pk):
                
                sql_create_query = self.sql_command(table_name, keys_dict, 'create', pk=pk)
            
            pointer.execute(sql_create_query)
            
            connection.commit()
            
            table_was_created = True

        except (Exception, psycopg2.Error) as error: #TODO: TRATAR EXCEÇÃO AQUI

            pointer.execute("ROLLBACK")
        
        pointer.close(); connection.close()
        
        return table_was_created


    def read_entries_from_table(self, table_name, **kwargs):
        
        """Busca no banco a tabela [table_name], retornando uma lista, tal que cada elemento representa
        uma linha (registro) da tabela, ou uma lista vazia, caso a tabela não seja encontrada. Pode retornar
        a tabela inteira, ou parte dos valores se forem informados, corretamente, os argumentos opcionais
        **kwargs.
        
        
        Keyword arguments:
        =================
        
        table_name  -- Nome da tabela a ser buscada
        
        **kwargs:
        =========
        
        field_key -- Chave (nome da coluna) do campo tomado como referência de ordenamento
        
        sort_type      -- Modo como os registros são buscados:
            
        *sort_type options*
        -----------------
            - DESC ==> Do maior para o menor
            - ASC  ==> Do menor para o maior
            
        limit     -- Número de registros que devem ser retornados
        """
        
        field_key = kwargs.get('field_key')
        
        sort_type = kwargs.get('sort_type')
        
        limit = str(kwargs.get('limit'))

        entries = []

        connection, pointer = self.create_connection()

        try:
            
            sql_select_query = 'SELECT * FROM ' + table_name
            
            if (field_key and sort_type and limit):
                
                sql_select_query = sql_select_query + ' ORDER BY ' + field_key + ' ' + sort_type + ' LIMIT ' + limit

            pointer.execute(sql_select_query)

            entries = pointer.fetchall()

        except (Exception, psycopg2.Error) as error: #TODO: TRATAR EXCEÇÃO AQUI (fazendo as entradas como 
                                                            #cunjunto vazio, já não seria uma forma de tratar a exceção?)

            pointer.execute("ROLLBACK")
                
        pointer.close(); connection.close()

        return entries


    def update_entry(self, table_name, pk_field, pk_value, field_to_update, new_field_value):
    
        """Dada uma certa tabela, atualiza uma entrada específica da mesma.
        
        
        Keyword arguments:
        =================
        
        table_name      -- Nome da tabela a ser buscada
        pk_field        -- Chave (nome da coluna) atribuída como chave primária; ``pk``, se chave default.
        pk_value        -- Valor da chave primária do registro
        field_to_update -- Chave (nome da coluna) do campo a ser regravado
        new_field_value -- Novo valor a ser gravado no campo
        """

        was_entry_updated = False

        connection, pointer = self.create_connection()

        try:

            sql_update_query = 'Update ' + table_name + ' set ' + field_to_update + '\
             = %s where ' + pk_field + '= %s'
                
            pointer.execute(sql_update_query, (new_field_value, pk_value))

            connection.commit()

            was_entry_updated = True

        except (Exception, psycopg2.Error) as error: #TODO: TRATAR EXCEÇÃO AQUI

            pointer.execute("ROLLBACK")

        pointer.close(); connection.close()

        return was_entry_updated


    def save_data_in_table(self, table_name, keys_dict, data):
        
        """Salva na tabela <table_name>, nas respectivas colunas <keys_dict.keys() [i]>, os dados correspondentes, <data>.
        
        
        Keyword arguments:
        =================
        
        table_name  -- Nome da tabela na qual os dados devem ser gravados
        keys_dict   -- Dicionário com as chaves (nomes das colunas) e valores (respectivos tipos dos dados das colunas)
        data        -- Lista de dados a serem gravados
        """

        data_was_saved_in_table = False

        connection, pointer = self.create_connection()
        
        try:
    
            sql_save_query = self.sql_command(table_name, keys_dict, 'save')
            
            for i in range(len(data)):
                
                pointer.execute(sql_save_query, data[i])
            
            connection.commit()
            
            data_was_saved_in_table = True

        except (Exception, psycopg2.Error) as error: #TODO: TRATAR EXCEÇÃO AQUI

            pointer.execute("ROLLBACK")
        
        pointer.close(); connection.close()

        return data_was_saved_in_table

        
        def delete_entries_on_table(self):
            pass

        
        def delete_table(self):
            pass