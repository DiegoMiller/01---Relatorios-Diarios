(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};


$logo=Import["marche.png"];


getGeraAJUSTES[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},

sql=" 	

SET NOCOUNT ON;	
exec [192.168.0.6].Zeus_rtg.[dbo].[SP_MARCHE_GeraAJUSTES_TRANSFERENCIAS_INTERNAS_EATALY]

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
	r
];
r20\[LeftArrow]getGeraAJUSTES[$lojas];


getGeraNF[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},

sql=" 	

SET NOCOUNT ON;	
exec [192.168.0.6].Zeus_rtg.[dbo].[SP_MARCHE_GeraNF_TRANSFERENCIAS_INTERNAS_EATALY]  

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
	r
];
r30\[LeftArrow]getGeraNF[$lojas];


getValidacoes[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},

sql=" 	

SET NOCOUNT ON;	
		
DECLARE @DTA_INI DATE = '2016-04-04' --DATA DE INICIO DO PROCESSO AUTOMATICO
DECLARE @DTA_FIM DATE = CONVERT(DATE,GETDATE()-1)

IF OBJECT_ID('tempdb..#TMP_VENDAS_VINHOS') IS NOT NULL		BEGIN 	DROP TABLE #TMP_VENDAS_VINHOS END

IF OBJECT_ID('tempdb..#TMP_VENDA_GARRAFAS') IS NOT NULL		BEGIN  	DROP TABLE #TMP_VENDA_GARRAFAS END

IF OBJECT_ID('tempdb..#TAB_TRANSF') IS NOT NULL				BEGIN	DROP TABLE #TAB_TRANSF END

IF OBJECT_ID('tempdb..#TAB_ESTORNO') IS NOT NULL			BEGIN 	DROP TABLE #TAB_ESTORNO END

IF OBJECT_ID('tempdb..#TMP_VENDA_OUTROS') IS NOT NULL		BEGIN	DROP TABLE #TMP_VENDA_OUTROS END

IF OBJECT_ID('tempdb..#TMP_RESULT') IS NOT NULL				BEGIN   DROP TABLE #TMP_RESULT END

IF object_id('tempdb..#TMP_TAB_AJUSTE_ESTOQUE') IS NOT NULL BEGIN DROP TABLE #TMP_TAB_AJUSTE_ESTOQUE END

IF object_id('tempdb..#TMP_VALIDACAO') IS NOT NULL BEGIN DROP TABLE #TMP_VALIDACAO END


			CREATE TABLE [#TMP_TAB_AJUSTE_ESTOQUE]
			(
				[COD_AJUSTE_ESTOQUE] [int] NOT NULL DEFAULT (0),
				[COD_PRODUTO] [varchar](13) NOT NULL,
				[COD_LOJA] [int] NOT NULL  DEFAULT (0),
				[COD_AJUSTE] [int] NOT NULL   DEFAULT (0),
				[QTD_AJUSTE] [float] NOT NULL  DEFAULT (0.00),
				[COD_FORNECEDOR] [int] NULL,
				[NUM_DOCUMENTO] [varchar](25) NULL,
				[QTD_ESTOQUE_ANT] [float] NOT NULL  DEFAULT (0.00),
				[QTD_ESTOQUE_POST] [float] NOT NULL  DEFAULT (0.00),
				[VAL_CUSTO_SEM_ICMS] [float] NOT NULL   DEFAULT (0.00),
				[VAL_CUSTO_MED] [float] NOT NULL  DEFAULT (0.00),
				[VAL_CUSTO_REP] [float] NOT NULL  DEFAULT (0.00),
				[DTA_AJUSTE] [datetime] NOT NULL,
				[DTA_INCLUSAO] [datetime] NOT NULL  DEFAULT (getdate()),
				[USUARIO] [varchar](35) NULL  DEFAULT (suser_sname()),
				[FLG_EFETIVADO] [varchar](1) NOT NULL DEFAULT ('N') , 
				recid int IDENTITY(1,1)
				)

------------------------------------------------------------------------
--BUSCANDO E CONVERTENDO TACAS E GARRAFAS
------------------------------------------------------------------------
SELECT 
		 A.COD_LOJA
		,COD_PRODUTO_GR COD_PRODUTO
		,SUM(CAST(QTDE_PRODUTO AS INT))/4 AS QTD_VENDA
INTO #TMP_VENDAS_VINHOS
FROM [192.168.0.13].[BI].[DBO].BI_VENDA_PRODUTO A with (nolock)
		LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO B  with (nolock)
				ON B.COD_PRODUTO = A.COD_PRODUTO 
		INNER JOIN [192.168.0.13].[BI].[DBO].[CADASTRO_DEPARA_VINHO] AS DEPARA  with (nolock)
				ON DEPARA.COD_PRODUTO_TC = A.COD_PRODUTO
WHERE 1=1 
		AND COD_LOJA = 33
		AND CONVERT(DATE,DATA) >= @DTA_INI
		AND CONVERT(DATE,DATA) <= @DTA_FIM
GROUP BY
		A.COD_LOJA
		,COD_PRODUTO_GR			
UNION 
SELECT 	 A.COD_LOJA
		,A.COD_PRODUTO
		,SUM(CAST(QTDE_PRODUTO AS DOUBLE PRECISION)) AS QTD_VENDA
FROM [192.168.0.13].[BI].[DBO].BI_VENDA_PRODUTO A  with (nolock)
		LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO B  with (nolock)
				ON B.COD_PRODUTO = A.COD_PRODUTO 
WHERE 1=1 
		AND COD_LOJA = 33
		AND CONVERT(DATE,DATA) >= @DTA_INI
		AND CONVERT(DATE,DATA) <= @DTA_FIM
		AND COD_DEPARTAMENTO IN (2)	
GROUP BY
		A.COD_LOJA
	   ,A.COD_PRODUTO
------------------------------------------------------------------------
--SOMANDO GARRAFAS
------------------------------------------------------------------------
SELECT 
		COD_PRODUTO,SUM(QTD_VENDA) AS QTD_VENDA
INTO #TMP_VENDA_GARRAFAS
FROM #TMP_VENDAS_VINHOS VENDAS

WHERE 1=1 
GROUP BY 
		COD_PRODUTO
------------------------------------------------------------------------
--TRANSF
------------------------------------------------------------------------	
SELECT 		
		 AJUSTE.COD_PRODUTO*1 COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_TRANSF
INTO  #TAB_TRANSF
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK)
WHERE 1=1
		AND COD_AJUSTE IN (281,13)
		AND COD_LOJA = 29 
		AND CONVERT(DATE,DTA_AJUSTE) >= @DTA_INI
		AND CONVERT(DATE,DTA_AJUSTE) <= @DTA_FIM --CONVERT(DATETIME,@DTA_FIM)-1
GROUP BY 
		AJUSTE.COD_PRODUTO		
------------------------------------------------------------------------
--ESTORNO
------------------------------------------------------------------------	
SELECT 		
		 AJUSTE.COD_PRODUTO*1 COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_ESTORNO
INTO  #TAB_ESTORNO
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK)
WHERE 1=1
		AND COD_AJUSTE IN (283)
		AND COD_LOJA = 29 
		AND CONVERT(DATE,DTA_AJUSTE) >= @DTA_INI
		AND CONVERT(DATE,DTA_AJUSTE) <= @DTA_FIM
GROUP BY 
		AJUSTE.COD_PRODUTO				
------------------------------------------------------------------------
--JUNTANDO DADOS
------------------------------------------------------------------------		
SELECT 
		A.COD_PRODUTO 
		,DESCRICAO
		,NO_DEPARTAMENTO 
		,QTD_VENDA
		,ISNULL(QTD_TRANSF,0) QTD_TRANSF
		,ISNULL(QTD_ESTORNO,0) QTD_ESTORNO
		,QTD_VENDA-(ISNULL(QTD_TRANSF,0)+ISNULL(QTD_ESTORNO,0)) A_TRANSF
INTO #TMP_RESULT
FROM #TMP_VENDA_GARRAFAS A 
	LEFT JOIN #TAB_TRANSF B 
			ON B.COD_PRODUTO = A.COD_PRODUTO
	LEFT JOIN #TAB_ESTORNO ESTORNO
			ON ESTORNo.COD_PRODUTO = A.COD_PRODUTO
	LEFT JOIN  [192.168.0.13].BI.DBO.BI_CAD_PRODUTO C  with (nolock)
			ON C.COD_PRODUTO = A.COD_PRODUTO 
-- WHERE 1=1
		--AND QTD_VENDA-(ISNULL(QTD_TRANSF,0)+ISNULL(QTD_ESTORNO,0)) <> 0
------------------------------------------------------------------------
--BUSCANDO PRODUTOS OUTROS
------------------------------------------------------------------------
SELECT 	 A.COD_LOJA
		,A.COD_PRODUTO
		,SUM(CAST(QTDE_PRODUTO AS DOUBLE PRECISION)) AS QTD_VENDA
INTO #TMP_VENDA_OUTROS
FROM [192.168.0.13].[BI].[DBO].BI_VENDA_PRODUTO A  with (nolock)
		LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO B  with (nolock)
				ON B.COD_PRODUTO = A.COD_PRODUTO 
WHERE 1=1 
		AND COD_LOJA = 33 
		AND CONVERT(DATE,DATA) >= @DTA_INI
		AND CONVERT(DATE,DATA) <= @DTA_FIM
		AND COD_DEPARTAMENTO NOT IN (2)
		AND A.COD_PRODUTO IN (SELECT DISTINCT V.COD_PRODUTO FROM  [192.168.0.13].[BI].[DBO].BI_VENDA_PRODUTO V  with (nolock) INNER JOIN [ZEUS_RTG].[DBO].[VW_MARCHE_ENTRADAS] E ON (E.COD_LOJA = V.COD_LOJA AND E.COD_PRODUTO = V.COD_PRODUTO) WHERE 1=1 AND V.COD_LOJA = 29 AND E.COD_LOJA = 29 AND V.COD_PRODUTO NOT IN (1019239,1020263,1027074))	 
GROUP BY
		A.COD_LOJA
	   ,A.COD_PRODUTO				
------------------------------------------------------------------------
--JUNTANDO DADOS
------------------------------------------------------------------------		
INSERT INTO #TMP_RESULT
SELECT
		 VENDAS.COD_PRODUTO
		,DESCRICAO
		,NO_DEPARTAMENTO 
		,QTD_VENDA
		,ISNULL(QTD_TRANSF,0) QTD_TRANSF
		,ISNULL(QTD_ESTORNO,0) QTD_ESTORNO
		,QTD_VENDA-(ISNULL(QTD_TRANSF,0)+ISNULL(QTD_ESTORNO,0)) A_TRANSF
FROM #TMP_VENDA_OUTROS VENDAS 
		LEFT JOIN #TAB_TRANSF TRANSF 
				ON TRANSF.COD_PRODUTO = VENDAS.COD_PRODUTO
		LEFT JOIN #TAB_ESTORNO ESTORNO
				ON ESTORNO.COD_PRODUTO = VENDAS.COD_PRODUTO
		LEFT JOIN  [192.168.0.13].BI.DBO.BI_CAD_PRODUTO C  with (nolock)
				ON C.COD_PRODUTO = VENDAS.COD_PRODUTO 
------------------------------------------------------------------------
--INSERINDO DADOS DO AJUSTE NA TABELA TEMPORARIA
------------------------------------------------------------------------	
INSERT INTO #TMP_TAB_AJUSTE_ESTOQUE ( [COD_LOJA] ,[COD_PRODUTO] , [COD_AJUSTE] , [QTD_AJUSTE], [DTA_AJUSTE])
SELECT 
		29 AS COD_LOJA
		, RIGHT('00000000' + CAST(COD_PRODUTO AS VARCHAR(10))  ,8)
		, 281 COD_AJUSTE
		, A_TRANSF * - 1  QTD_AJUSTE
		, CAST(GETDATE() - 1 AS DATE) DTA_AJUSTE 
FROM #TMP_RESULT
	WHERE 1=1
		AND  A_TRANSF > 0 										

----------------------------------------------------------------------
--DELETANDO AJUSTES JA EXISTENTES DA LISTA DE AJUSTES
----------------------------------------------------------------------	
DELETE A
FROM #TMP_TAB_AJUSTE_ESTOQUE AS A , ZEUS_RTG.DBO.TAB_AJUSTE_ESTOQUE AS B
WHERE 1 =1
		AND B.QTD_AJUSTE = A.QTD_AJUSTE
		AND B.COD_AJUSTE = A.COD_AJUSTE
		AND B.DTA_AJUSTE = A.DTA_AJUSTE
		AND B.COD_PRODUTO = A.COD_PRODUTO
		AND B.COD_LOJA = A.COD_LOJA

----------------------------------------------------------------------
--VALIDACOES
----------------------------------------------------------------------	
CREATE TABLE   [#TMP_VALIDACAO]
			(
				Validacao varchar(max)
				,Status varchar(max)
				,Dta date
				)


DECLARE @ULT_TRANFS DATE
SELECT  @ULT_TRANFS = ISNULL(MAX(CONVERT(DATE,dta_ajuste)),'2014-01-01') FROM TAB_AJUSTE_ESTOQUE  WITH (NOLOCK) 		WHERE 1=1	AND COD_AJUSTE IN  (281) AND COD_LOJA = 29
	

	 		 

IF EXISTS  (SELECT 1 FROM (SELECT  COUNT(COD_PRODUTO) COD_PRODUTO FROM #TMP_TAB_AJUSTE_ESTOQUE) TMP WHERE COD_PRODUTO = 1)  

BEGIN	

		insert into [#TMP_VALIDACAO] (Validacao, Status, Dta)
		Select 'Transfer\[EHat]ncias' , 'Existe 1 pend\[EHat]ncia de transfer\[EHat]ncia' as VALICACAO , @ULT_TRANFS 

END 


IF EXISTS  (SELECT 1 FROM (SELECT  COUNT(COD_PRODUTO) COD_PRODUTO FROM #TMP_TAB_AJUSTE_ESTOQUE ) TMP WHERE COD_PRODUTO > 1)  

BEGIN	
		insert into [#TMP_VALIDACAO] (Validacao, Status, Dta)
		SELECT  'Transfer\[EHat]ncias' ,'Existem ' + convert(varchar,COUNT(COD_PRODUTO)) +' pend\[EHat]ncias de transfer\[EHat]ncias' as VALICACAO , @ULT_TRANFS  FROM #TMP_TAB_AJUSTE_ESTOQUE
END 

ELSE 
BEGIN
		insert into [#TMP_VALIDACAO] (Validacao, Status, Dta)
		select  'Transfer\[EHat]ncias' ,'N\[ATilde]o existe pend\[EHat]ncias de transfer\[EHat]ncia' AS VALICACAO,  @ULT_TRANFS 
END

IF OBJECT_ID('tempdb..#TMP_TRANSF') IS NOT NULL		BEGIN		DROP TABLE #TMP_TRANSF END

IF OBJECT_ID('tempdb..#TAB_EMITIDA') IS NOT NULL	BEGIN		DROP TABLE #TAB_EMITIDA END

IF OBJECT_ID('tempdb..#TMP_CUSTO') IS NOT NULL		BEGIN		DROP TABLE #TMP_CUSTO END

IF OBJECT_ID('tempdb..#TAB_NFD') IS NOT NULL		BEGIN		DROP TABLE #TAB_NFD END

IF OBJECT_ID('tempdb..#TAB_ESTORNO') IS NOT NULL	BEGIN 		DROP TABLE #TAB_ESTORNO END

IF OBJECT_ID('tempdb..#TAB_ESTORNO_NF') IS NOT NULL	BEGIN 		DROP TABLE #TAB_ESTORNO_NF END

IF OBJECT_ID('tempdb..#TMP_EMISSAO') IS NOT NULL	BEGIN 		DROP TABLE #TMP_EMISSAO END

---------------------------------------------------		
--BUSCANDO TRANSFENRENCIAS
---------------------------------------------------				
SELECT 
		COD_LOJA
		,COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_TRANSF
		,SUM((QTD_AJUSTE*VAL_CUSTO_REP)*-1) AS CUSTO_TRANSF
		INTO #TMP_TRANSF
FROM TAB_AJUSTE_ESTOQUE  WITH (NOLOCK) 
		WHERE 1=1
		AND COD_AJUSTE IN  (269,281)
		AND COD_LOJA = 29
GROUP BY 
		COD_LOJA
		,COD_PRODUTO
------------------------------------------------------------------------
--ESTORNO
------------------------------------------------------------------------	
SELECT 		
		 AJUSTE.COD_PRODUTO*1 COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION)) AS QTD_ESTORNO
INTO  #TAB_ESTORNO_NF
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK)
WHERE 1=1
		AND COD_AJUSTE IN (283)
		AND COD_LOJA = 29 

GROUP BY 
		AJUSTE.COD_PRODUTO		
---------------------------------------------------		
--VENDAS NF EMISSAO--
---------------------------------------------------	
		SELECT 
		COD_PRODUTO
		,SUM(CAST((QTD_EMBALAGEM*QTD_ENTRADA) AS DOUBLE PRECISION)) AS QTD_EMITIDA
INTO #TAB_EMITIDA
FROM [ZEUS_RTG].[DBO].[VW_NF_SAIDA]
WHERE 1=1
		AND COD_LOJA in (29)
		AND COD_CLIENTE = 1960502441		
		AND COD_FISCAL = 192	
GROUP BY 
		COD_PRODUTO
---------------------------------------------------		
--BUSCANDO NFD'S
---------------------------------------------------		
SELECT 
		COD_PRODUTO*1 COD_PRODUTO
		,SUM(CAST((QTD_EMBALAGEM*QTD_ENTRADA) AS DOUBLE PRECISION)) AS QTD_DEVOLVIDA
INTO #TAB_NFD
FROM [192.168.0.6].[ZEUS_RTG].[DBO].VW_NF_DEVOLUCAO
WHERE 1=1
		AND COD_LOJA = 33
		AND COD_FORNECEDOR = 102856
GROUP BY 
		COD_PRODUTO
---------------------------------------------------		
--BUSCANDO CUSTOS--
---------------------------------------------------		
SELECT * 
INTO #TMP_CUSTO 
FROM [192.168.0.13].DW.DBO.PRODUTO_CUSTOS 
WHERE 1=1
		AND COD_PRODUTO IN (SELECT DISTINCT COD_PRODUTO FROM #TMP_TRANSF)
		AND COD_LOJA in (29) 
---------------------------------------------------		
--JUNTANDO DADOS--
---------------------------------------------------		
SELECT 
		TRANSF.COD_PRODUTO*1 AS COD_PRODUTO
		,DESCRICAO
		,NO_DEPARTAMENTO
		,ISNULL(UNIDADE_VENDA,'UN') AS UNIDADE_VENDA
		,QTD_TRANSF
		,ISNULL(QTD_ESTORNO,0)  AS QTD_ESTORNO
		,QTD_TRANSF-ISNULL(QTD_ESTORNO,0) AS QTD_TRANSF_TOTAL
		,ISNULL(QTD_EMITIDA,0)  AS QTD_EMITIDA
		,ISNULL(QTD_DEVOLVIDA,0) AS QTD_DEVOLVIDA
		,ISNULL(QTD_EMITIDA,0)-ISNULL(QTD_DEVOLVIDA,0) AS QTD_EMITIDA_TOTAL
		,(QTD_TRANSF-ISNULL(QTD_ESTORNO,0)) - (ISNULL(QTD_EMITIDA,0)-ISNULL(QTD_DEVOLVIDA,0)) A_EMITIR
		,CAST(ISNULL(CUSTO,0) AS DOUBLE PRECISION) CUSTO
INTO  #TMP_EMISSAO 
FROM #TMP_TRANSF AS TRANSF 
        LEFT JOIN #TAB_ESTORNO_NF ESTORNO
				ON ESTORNO.COD_PRODUTO = TRANSF.COD_PRODUTO
		LEFT JOIN #TAB_EMITIDA EMITIDA
				ON EMITIDA.COD_PRODUTO = TRANSF.COD_PRODUTO
		LEFT JOIN #TAB_NFD NFD
				ON NFD.COD_PRODUTO = TRANSF.COD_PRODUTO
		LEFT JOIN #TMP_CUSTO CUSTO 
				ON CUSTO.COD_PRODUTO = TRANSF.COD_PRODUTO
		LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO AS CAD 
				ON  CAD.COD_PRODUTO = TRANSF.COD_PRODUTO



DECLARE @ULT_EMISSAO DATE

SELECT  @ULT_EMISSAO = ISNULL(MAX(CONVERT(DATE,dta_emissao)),'2014-01-01') FROM [ZEUS_RTG].[DBO].[VW_NF_SAIDA] WHERE 1=1 AND COD_LOJA in (29) AND COD_CLIENTE = 1960502441 AND COD_FISCAL = 192	
		 
	

IF EXISTS  (SELECT 1 FROM (SELECT  COUNT(COD_PRODUTO) COD_PRODUTO FROM #TMP_EMISSAO WHERE A_EMITIR > 0.01) TMP WHERE COD_PRODUTO = 1)  

BEGIN	
		insert into [#TMP_VALIDACAO] (Validacao, Status, Dta)
		Select 'Emiss\[ATilde]o' , 'Existe 1 produto aguardando emiss\[ATilde]o de NF' as VALICACAO , @ULT_EMISSAO
END 


IF EXISTS  (SELECT 1 FROM (SELECT  COUNT(COD_PRODUTO) COD_PRODUTO FROM #TMP_EMISSAO WHERE A_EMITIR > 0.01) TMP WHERE COD_PRODUTO > 1)  

BEGIN	
		insert into [#TMP_VALIDACAO] (Validacao, Status, Dta)
		SELECT  'Emiss\[ATilde]o' ,'Existem ' + convert(varchar,COUNT(COD_PRODUTO)) +' produtos aguardando emiss\[ATilde]o de NF' as VALICACAO , @ULT_EMISSAO FROM #TMP_EMISSAO WHERE A_EMITIR > 0.01
END 

ELSE 
BEGIN
		insert into [#TMP_VALIDACAO] (Validacao, Status, Dta)
		select  'Emiss\[ATilde]o' ,'N\[ATilde]o existe pend\[EHat]ncias de emiss\[ATilde]o de NF' AS VALICACAO, @ULT_EMISSAO
END

select 
Validacao [Valida\[CCedilla]\[ATilde]o]
,Status   
,convert(varchar,dta,103) [Data Ult Mov]
 from [#TMP_VALIDACAO] 


";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
	r
(*
$Transf=ToString@r["Data"][[1,2]];
$Emissao=ToString@r["Data"][[2,2]];
*)

];
r\[LeftArrow]getValidacoes[$lojas]


gridStatus[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
	 #=="N\[ATilde]o existe pend\[EHat]ncias de emiss\[ATilde]o de NF",Darker@Green
	,True,Red
	]&;


    stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="0"; 

	grid=maReportGrid[r,"ColumnFormat"->{{}-> stl},"TotalRow"->False];
	title=Style[Row@{""},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
gridStatus[r]


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

createReport[data_Symbol]:=Module[{s},
	s\[LeftArrow]data;
	Grid[{{gridStatus[r](*,gridDuplicadas[r10]*)}},Spacings->{1.5,4},Frame->Transparent]
]
createReport[s];
rep=marcheTemplate[grid=Row@{"Controle de Transfer\[EHat]ncias Autom\[AAcute]ticas Eataly em ",$now},createReport[s],1200, $logo]

$fileName1="Controle de Transfer\[EHat]ncias Autom\[AAcute]ticas Eataly em "<>$now2<>".png";
Export["Controle de Transfer\[EHat]ncias Autom\[AAcute]ticas Eataly em "<>$now2<>".png",rep];


getTransfExcel[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},
sql=" 
	
SET NOCOUNT ON;	

DECLARE @DTA_INI DATE = '2016-04-04' --DATA DE INICIO DO PROCESSO AUTOMATICO
DECLARE @DTA_FIM DATE = CONVERT(DATE,GETDATE()-1)

IF OBJECT_ID('tempdb..#TMP_VENDAS_VINHOS') IS NOT NULL		BEGIN 	DROP TABLE #TMP_VENDAS_VINHOS END

IF OBJECT_ID('tempdb..#TMP_VENDA_GARRAFAS') IS NOT NULL		BEGIN  	DROP TABLE #TMP_VENDA_GARRAFAS END

IF OBJECT_ID('tempdb..#TAB_TRANSF') IS NOT NULL				BEGIN	DROP TABLE #TAB_TRANSF END

IF OBJECT_ID('tempdb..#TAB_ESTORNO') IS NOT NULL			BEGIN 	DROP TABLE #TAB_ESTORNO END

IF OBJECT_ID('tempdb..#TMP_VENDA_OUTROS') IS NOT NULL		BEGIN	DROP TABLE #TMP_VENDA_OUTROS END

IF OBJECT_ID('tempdb..#TMP_RESULT') IS NOT NULL				BEGIN   DROP TABLE #TMP_RESULT END

IF object_id('tempdb..#TMP_VALIDACAO') IS NOT NULL BEGIN DROP TABLE #TMP_VALIDACAO END

------------------------------------------------------------------------
--BUSCANDO E CONVERTENDO TACAS E GARRAFAS
------------------------------------------------------------------------
SELECT 
		 A.COD_LOJA
		,COD_PRODUTO_GR COD_PRODUTO
		,SUM(CAST(QTDE_PRODUTO AS INT))/4 AS QTD_VENDA
INTO #TMP_VENDAS_VINHOS
FROM [192.168.0.13].[BI].[DBO].BI_VENDA_PRODUTO A with (nolock)
		LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO B  with (nolock)
				ON B.COD_PRODUTO = A.COD_PRODUTO 
		INNER JOIN [192.168.0.13].[BI].[DBO].[CADASTRO_DEPARA_VINHO] AS DEPARA  with (nolock)
				ON DEPARA.COD_PRODUTO_TC = A.COD_PRODUTO
WHERE 1=1 
		AND COD_LOJA = 33
		AND CONVERT(DATE,DATA) >= @DTA_INI
		AND CONVERT(DATE,DATA) <= @DTA_FIM
GROUP BY
		A.COD_LOJA
		,COD_PRODUTO_GR			
UNION 
SELECT 	 A.COD_LOJA
		,A.COD_PRODUTO
		,SUM(CAST(QTDE_PRODUTO AS DOUBLE PRECISION)) AS QTD_VENDA
FROM [192.168.0.13].[BI].[DBO].BI_VENDA_PRODUTO A  with (nolock)
		LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO B  with (nolock)
				ON B.COD_PRODUTO = A.COD_PRODUTO 
WHERE 1=1 
		AND COD_LOJA = 33
		AND CONVERT(DATE,DATA) >= @DTA_INI
		AND CONVERT(DATE,DATA) <= @DTA_FIM
		AND COD_DEPARTAMENTO IN (2)	
GROUP BY
		A.COD_LOJA
	   ,A.COD_PRODUTO
------------------------------------------------------------------------
--SOMANDO GARRAFAS
------------------------------------------------------------------------
SELECT 
		COD_PRODUTO,SUM(QTD_VENDA) AS QTD_VENDA
INTO #TMP_VENDA_GARRAFAS
FROM #TMP_VENDAS_VINHOS VENDAS

WHERE 1=1 
GROUP BY 
		COD_PRODUTO
------------------------------------------------------------------------
--TRANSF
------------------------------------------------------------------------	
SELECT 		
		 AJUSTE.COD_PRODUTO*1 COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_TRANSF
INTO  #TAB_TRANSF
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK)
WHERE 1=1
		AND COD_AJUSTE IN (281,13)
		AND COD_LOJA = 29 
		AND CONVERT(DATE,DTA_AJUSTE) >= @DTA_INI
		AND CONVERT(DATE,DTA_AJUSTE) <= @DTA_FIM -- CONVERT(DATETIME,@DTA_FIM)-1
GROUP BY 
		AJUSTE.COD_PRODUTO		
------------------------------------------------------------------------
--ESTORNO
------------------------------------------------------------------------	
SELECT 		
		 AJUSTE.COD_PRODUTO*1 COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_ESTORNO
INTO  #TAB_ESTORNO
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK)
WHERE 1=1
		AND COD_AJUSTE IN (283)
		AND COD_LOJA = 29 
		AND CONVERT(DATE,DTA_AJUSTE) >= @DTA_INI
		AND CONVERT(DATE,DTA_AJUSTE) <= @DTA_FIM
GROUP BY 
		AJUSTE.COD_PRODUTO				
------------------------------------------------------------------------
--JUNTANDO DADOS
------------------------------------------------------------------------		
SELECT 
		A.COD_PRODUTO 
		,DESCRICAO
		,NO_DEPARTAMENTO 
		,QTD_VENDA
		,ISNULL(QTD_TRANSF,0) QTD_TRANSF
		,ISNULL(QTD_ESTORNO,0) QTD_ESTORNO
		,QTD_VENDA-(ISNULL(QTD_TRANSF,0)+ISNULL(QTD_ESTORNO,0)) A_TRANSF
INTO #TMP_RESULT
FROM #TMP_VENDA_GARRAFAS A 
	LEFT JOIN #TAB_TRANSF B 
			ON B.COD_PRODUTO = A.COD_PRODUTO
	LEFT JOIN #TAB_ESTORNO ESTORNO
			ON ESTORNo.COD_PRODUTO = A.COD_PRODUTO
	LEFT JOIN  [192.168.0.13].BI.DBO.BI_CAD_PRODUTO C  with (nolock)
			ON C.COD_PRODUTO = A.COD_PRODUTO 
-- WHERE 1=1
		--AND QTD_VENDA-(ISNULL(QTD_TRANSF,0)+ISNULL(QTD_ESTORNO,0)) <> 0
------------------------------------------------------------------------
--BUSCANDO PRODUTOS OUTROS
------------------------------------------------------------------------
SELECT 	 A.COD_LOJA
		,A.COD_PRODUTO
		,SUM(CAST(QTDE_PRODUTO AS DOUBLE PRECISION)) AS QTD_VENDA
INTO #TMP_VENDA_OUTROS
FROM [192.168.0.13].[BI].[DBO].BI_VENDA_PRODUTO A  with (nolock)
		LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO B  with (nolock)
				ON B.COD_PRODUTO = A.COD_PRODUTO 
WHERE 1=1 
		AND COD_LOJA = 33 
		AND CONVERT(DATE,DATA) >= @DTA_INI
		AND CONVERT(DATE,DATA) <= @DTA_FIM
		AND COD_DEPARTAMENTO NOT IN (2)
		AND A.COD_PRODUTO IN (SELECT DISTINCT V.COD_PRODUTO FROM  [192.168.0.13].[BI].[DBO].BI_VENDA_PRODUTO V  with (nolock) INNER JOIN [ZEUS_RTG].[DBO].[VW_MARCHE_ENTRADAS] E ON (E.COD_LOJA = V.COD_LOJA AND E.COD_PRODUTO = V.COD_PRODUTO) WHERE 1=1 AND V.COD_LOJA = 29 AND E.COD_LOJA = 29 AND V.COD_PRODUTO NOT IN (1019239,1020263,1027074))	 
GROUP BY
		A.COD_LOJA
	   ,A.COD_PRODUTO				
------------------------------------------------------------------------
--JUNTANDO DADOS
------------------------------------------------------------------------		
INSERT INTO #TMP_RESULT
SELECT
		 VENDAS.COD_PRODUTO
		,DESCRICAO
		,NO_DEPARTAMENTO 
		,QTD_VENDA
		,ISNULL(QTD_TRANSF,0) QTD_TRANSF
		,ISNULL(QTD_ESTORNO,0) QTD_ESTORNO
		,QTD_VENDA-(ISNULL(QTD_TRANSF,0)+ISNULL(QTD_ESTORNO,0)) A_TRANSF
FROM #TMP_VENDA_OUTROS VENDAS 
		LEFT JOIN #TAB_TRANSF TRANSF 
				ON TRANSF.COD_PRODUTO = VENDAS.COD_PRODUTO
		LEFT JOIN #TAB_ESTORNO ESTORNO
				ON ESTORNO.COD_PRODUTO = VENDAS.COD_PRODUTO
		LEFT JOIN  [192.168.0.13].BI.DBO.BI_CAD_PRODUTO C  with (nolock)
				ON C.COD_PRODUTO = VENDAS.COD_PRODUTO 
------------------------------------------------------------------------
--INSERINDO DADOS DO AJUSTE NA TABELA TEMPORARIA
------------------------------------------------------------------------	

SELECT * FROM #TMP_RESULT ORDER BY A_TRANSF DESC

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
	r
];

r3\[LeftArrow]getTransfExcel[$lojas]


$fileNameTransfExcel="Relacao de Transferencias em "<>$now2<>".xlsx";
Export["Relacao de Transferencias em "<>$now2<>".xlsx",r3["DataAll"]];		


getEmissaofExcel[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},

sql=" 
	
SET NOCOUNT ON;	

IF OBJECT_ID('tempdb..#TMP_TRANSF') IS NOT NULL		BEGIN		DROP TABLE #TMP_TRANSF END

IF OBJECT_ID('tempdb..#TAB_EMITIDA') IS NOT NULL	BEGIN		DROP TABLE #TAB_EMITIDA END

IF OBJECT_ID('tempdb..#TMP_CUSTO') IS NOT NULL		BEGIN		DROP TABLE #TMP_CUSTO END

IF OBJECT_ID('tempdb..#TAB_NFD') IS NOT NULL		BEGIN		DROP TABLE #TAB_NFD END

IF OBJECT_ID('tempdb..#TAB_ESTORNO') IS NOT NULL	BEGIN 		DROP TABLE #TAB_ESTORNO END

IF OBJECT_ID('tempdb..#TAB_ESTORNO_NF') IS NOT NULL	BEGIN 		DROP TABLE #TAB_ESTORNO_NF END

IF OBJECT_ID('tempdb..#TMP_EMISSAO') IS NOT NULL	BEGIN 		DROP TABLE #TMP_EMISSAO END

---------------------------------------------------		
--BUSCANDO TRANSFENRENCIAS
---------------------------------------------------				
SELECT 
		COD_LOJA
		,COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_TRANSF
		,SUM((QTD_AJUSTE*VAL_CUSTO_REP)*-1) AS CUSTO_TRANSF
		INTO #TMP_TRANSF
FROM TAB_AJUSTE_ESTOQUE  WITH (NOLOCK) 
		WHERE 1=1
		AND COD_AJUSTE IN  (269,281,13)
		AND COD_LOJA = 29
GROUP BY 
		COD_LOJA
		,COD_PRODUTO
------------------------------------------------------------------------
--ESTORNO
------------------------------------------------------------------------	
SELECT 		
		 AJUSTE.COD_PRODUTO*1 COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION)) AS QTD_ESTORNO
INTO  #TAB_ESTORNO_NF
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK)
WHERE 1=1
		AND COD_AJUSTE IN (283)
		AND COD_LOJA = 29 

GROUP BY 
		AJUSTE.COD_PRODUTO		
---------------------------------------------------		
--VENDAS NF EMISSAO--
---------------------------------------------------	
		SELECT 
		COD_PRODUTO
		,SUM(CAST((QTD_EMBALAGEM*QTD_ENTRADA) AS DOUBLE PRECISION)) AS QTD_EMITIDA
INTO #TAB_EMITIDA
FROM [ZEUS_RTG].[DBO].[VW_NF_SAIDA]
WHERE 1=1
		AND COD_LOJA in (29)
		AND COD_CLIENTE = 1960502441		
		AND COD_FISCAL = 192	
GROUP BY 
		COD_PRODUTO
---------------------------------------------------		
--BUSCANDO NFD'S
---------------------------------------------------		
SELECT 
		COD_PRODUTO*1 COD_PRODUTO
		,SUM(CAST((QTD_EMBALAGEM*QTD_ENTRADA) AS DOUBLE PRECISION)) AS QTD_DEVOLVIDA
INTO #TAB_NFD
FROM [192.168.0.6].[ZEUS_RTG].[DBO].VW_NF_DEVOLUCAO
WHERE 1=1
		AND COD_LOJA = 33
		AND COD_FORNECEDOR = 102856
GROUP BY 
		COD_PRODUTO
---------------------------------------------------		
--BUSCANDO CUSTOS--
---------------------------------------------------		
SELECT * 
INTO #TMP_CUSTO 
FROM [192.168.0.13].DW.DBO.PRODUTO_CUSTOS 
WHERE 1=1
		AND COD_PRODUTO IN (SELECT DISTINCT COD_PRODUTO FROM #TMP_TRANSF)
		AND COD_LOJA in (29) 
---------------------------------------------------		
--JUNTANDO DADOS--
---------------------------------------------------		
SELECT 
		TRANSF.COD_PRODUTO*1 AS COD_PRODUTO
		,DESCRICAO
		,NO_DEPARTAMENTO
		,ISNULL(UNIDADE_VENDA,'UN') AS UNIDADE_VENDA
		,QTD_TRANSF
		,ISNULL(QTD_ESTORNO,0)  AS QTD_ESTORNO
		,QTD_TRANSF-ISNULL(QTD_ESTORNO,0) AS QTD_TRANSF_TOTAL
		,ISNULL(QTD_EMITIDA,0)  AS QTD_EMITIDA
		,ISNULL(QTD_DEVOLVIDA,0) AS QTD_DEVOLVIDA
		,ISNULL(QTD_EMITIDA,0)-ISNULL(QTD_DEVOLVIDA,0) AS QTD_EMITIDA_TOTAL
		,(QTD_TRANSF-ISNULL(QTD_ESTORNO,0)) - (ISNULL(QTD_EMITIDA,0)-ISNULL(QTD_DEVOLVIDA,0)) A_EMITIR
		,CAST(ISNULL(CUSTO,0) AS DOUBLE PRECISION) CUSTO
INTO  #TMP_EMISSAO 
FROM #TMP_TRANSF AS TRANSF 
        LEFT JOIN #TAB_ESTORNO_NF ESTORNO
				ON ESTORNO.COD_PRODUTO = TRANSF.COD_PRODUTO
		LEFT JOIN #TAB_EMITIDA EMITIDA
				ON EMITIDA.COD_PRODUTO = TRANSF.COD_PRODUTO
		LEFT JOIN #TAB_NFD NFD
				ON NFD.COD_PRODUTO = TRANSF.COD_PRODUTO
		LEFT JOIN #TMP_CUSTO CUSTO 
				ON CUSTO.COD_PRODUTO = TRANSF.COD_PRODUTO
		LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO AS CAD 
				ON  CAD.COD_PRODUTO = TRANSF.COD_PRODUTO

SELECT * FROM #TMP_EMISSAO WHERE 1=1 AND CONVERT(DECIMAL,A_EMITIR) <> 0 ORDER BY A_EMITIR DESC

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
	r
];

r4\[LeftArrow]getEmissaofExcel[$lojas]


$fileNameEmissaoExcel="Relacao de emissao em "<>$now2<>".xlsx";
Export["Relacao de emissao em "<>$now2<>".xlsx",r4["DataAll"]];		


marcheMail[
  		"[GE] Controle de Transfer\[EHat]ncias Automaticas Eataly em " <>$now
  		,"Relatorio beta para valida\[CCedilla]\[ATilde]o do processo automatico."
		  ,$mailsGerencia
		  ,{$fileName1,$fileNameTransfExcel,$fileNameEmissaoExcel}
]


(*"diego.miller@marche.com.br"*)
