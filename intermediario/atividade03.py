#Atividade 6/10: Data Warehousing e Cloud Computing (Pilar 5)
#
#Agora vamos simular a fase de staging (armazenamento temporário) e o processo de Schema Evolution (evolução do esquema) no Cloud.
#
#Tarefa:
#
#
#
#Simulação de Data Lake (Cloud Storage): Simule o upload dos dados brutos do JSON para um Data Lake.
#
#Escreva uma função chamada simular_upload_s3(nome_arquivo_local) que recebe o nome do arquivo JSON e imprime a mensagem de que ele foi "movido para o bucket S3 na pasta raw/".
#
#Evolução do Schema: Em engenharia de dados, novos campos são frequentemente adicionados. A API de notícias pode adicionar o campo author (autor da notícia).
#
#Modifique a sua função task_extrair_dados_brutos para garantir que, mesmo que o campo author não exista em algumas notícias, ele seja adicionado ao DataFrame limpo (com o valor None ou Vazio) para manter o esquema consistente.
#
#Justificativa: Explique em texto, de forma concisa, por que é crucial garantir que um DataFrame tenha o mesmo schema (mesmas colunas) antes de carregá-lo em um Data Lake ou Data Warehouse.
#
#Para a entrega, envie:
#
#
#
#O código Python com a nova função simular_upload_s3.
#
#A modificação na função task_extrair_dados_brutos para incluir a coluna author.
#
#A justificativa concisa sobre a importância de manter o schema consistente.
#
#Este passo cobre o pilar de Cloud Computing e a fase inicial de Staging.

