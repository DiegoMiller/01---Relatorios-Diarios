(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$DtaIni={2016,08,05};
$lojas=33;


SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["marche.png"];
SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};
$NotasAjustadas = {2620,2629,2633,2636,2649,2654,2660,2681,2703};


getRODALANCAMENTOS[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},
sql=" 	

SET NOCOUNT ON;	

UPDATE
[INTRANET].[DBO].[TAB_TRANSFINTERNAS]
SET STATUS = 5 , APROVADOR = 'AUTOMATICO' , DTAPROVACAO = GETDATE()
WHERE 1=1
		AND COD_LOJA = 33
		AND CONVERT(DATE,DATA) >= CONVERT(DATE,'2016-08-05')
		AND STATUS = 6

DECLARE @DTA_HOJE DATE SET @DTA_HOJE = CONVERT(DATE,GETDATE())
EXEC [192.168.0.6].INTRANET.[DBO].[SP_MARCHE_INTEGRA_AJUSTES] '2016-01-01' , @DTA_HOJE
SELECT 1

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
	r
];
r20\[LeftArrow]getRODALANCAMENTOS[$lojas];


getGeraAJUSTES[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},
sql=" 	

SET NOCOUNT ON;	
EXEC [192.168.0.6].ZEUS_RTG.[DBO].[SP_MARCHE_GERAAJUSTES_TRANSFERENCIAS_INTERNAS_EATALY]
SELECT 1

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
	r
];
r20\[LeftArrow]getGeraAJUSTES[$lojas];


getGeraNF[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},
sql=" 	

SET NOCOUNT ON;	
EXEC [192.168.0.6].ZEUS_RTG.[DBO].[SP_MARCHE_GERANF_TRANSFERENCIAS_INTERNAS_EATALY]  
SELECT 1

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
	r
];
r30\[LeftArrow]getGeraNF[$lojas];


getEstoqueResumo[]:=Module[{sql,r,conn=marcheConn2[],tab},
sql=" 

SET NOCOUNT ON;	

SELECT NO_CLASSIF Class, NO_SUB_CLASSIF SubClass, SUM(CUSTO) [Custo Estoque], count(COD_PRODUTO) [Qtde Produtos], SUM(CUSTO_ZERO) [Produtos Custo Zero] 
FROM 
(

		SELECT
				E.COD_LOJA
				,E.COD_PRODUTO
				,DESCRICAO
				,NO_DEPARTAMENTO
				,NO_SECAO
				,NO_GRUPO
				,NO_SUBGRUPO
				,NO_CLASSIF	
				,NO_SUB_CLASSIF
				,CAST(QTD_ESTOQUE AS DOUBLE PRECISION) QTD_ESTOQUE
				,QTD_ESTOQUE*ISNULL(CAST(CUSTO AS DOUBLE PRECISION),0) CUSTO
				,CASE WHEN ISNULL(CUSTO,0) = 0 THEN 1 ELSE 0 END CUSTO_ZERO

		FROM DW.DBO.ESTOQUE E
				LEFT JOIN DW.DBO.PRODUTO_CUSTOS CUSTO
						ON CUSTO.COD_LOJA = E.COD_LOJA AND CUSTO.COD_PRODUTO = E.COD_PRODUTO
				LEFT JOIN BI.DBO.BI_CAD_PRODUTO CAD
						ON CAD.COD_PRODUTO = E.COD_PRODUTO
				LEFT JOIN BI.dbo.EAT_DEPARA_CLASSIF_DASH CLASS
						ON 1=1
						AND CLASS.COD_DEPARTAMENTO = CAD.COD_DEPARTAMENTO	
						AND CLASS.COD_SECAO = CAD.COD_SECAO	
						AND CLASS.COD_GRUPO = CAD.COD_GRUPO	
						AND CLASS.COD_SUB_GRUPO = CAD.COD_SUB_GRUPO
		WHERE 1=1
				AND E.COD_LOJA = 33 
				AND QTD_ESTOQUE <> 0
				AND CONVERT(DATE,DATA) = CONVERT(DATE,GETDATE())
) TMP

		GROUP BY 
				NO_CLASSIF
			   ,NO_SUB_CLASSIF		

		ORDER BY 
				NO_CLASSIF
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql(*,{SQLDateTime@@DtaIni}*)];
	r
];

(*r1\[LeftArrow]getEstoqueResumo[];*)


gridLojas[data_Symbol]:=Module[{grid,title,r,color2,stl,total},
	r\[LeftArrow]data;
		
	r1\[LeftArrow]getEstoqueResumo[];
	
	   total={"Total"
		,
		,Total@r[[All,"Custo Estoque"]]
		,Total@r[[All,"Qtde Produtos"]]
		,Total@r[[All,"Produtos Custo Zero"]]};
       r["DataAll"]=Join[r["DataAll"],{total}];
	
	color1=Which[ 
	 #>0,Darker@Green
	,True,Red
	]&;

    stl[val_]:=Style[maF["N"][val],Bold,color1@val];
	stl[Null]="0"; 

	color2=Which[ 
	 #>7,Black
	,True,Darker@Green
	]&;

    stl2[val_]:=Style[maF["N,00"][val],color2@val];
	stl2[Null]="0"; 

	grid=maReportGrid[r,"ColumnFormat"->{{"Custo Estoque"}-> stl},"TotalRow"->True];
	title=Style[Row@{"Estoque"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
gridLojas[r1];


getCorrigeCUstos[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},
sql=" 	

SET NOCOUNT ON;	

IF OBJECT_ID('TEMPDB..#TMP_CUSTOS') IS NOT NULL					BEGIN 	DROP TABLE #TMP_CUSTOS END
IF OBJECT_ID('TEMPDB..#TMP_CUSTO_DW') IS NOT NULL				BEGIN 	DROP TABLE #TMP_CUSTO_DW END

-------------------------------------------------------------------------------------------------------
--BUSCANDO CUSTOS NA [DW].[DBO].PRODUTO_CUSTOS
-------------------------------------------------------------------------------------------------------

SELECT * 
INTO #TMP_CUSTO_DW
FROM 
		[192.168.0.13].[DW].[DBO].PRODUTO_CUSTOS
WHERE 1=1 
		AND COD_LOJA = 33
		AND CUSTO IS NOT NULL

-------------------------------------------------------------------------------------------------------
--BUSCANDO CUSTO TABELA DO FORNECEDOR PRINCIPAL
-------------------------------------------------------------------------------------------------------

SELECT 33 AS COD_LOJA , COD_PRODUTO , COD_FORNECEDOR , VLR_EMB_COMPRA , QTD_EMB_COMPRA, VLR_UNITARIO
INTO #TMP_CUSTOS
FROM (
SELECT  
		COD_LOJA
		,A.COD_PRODUTO
		,A.COD_FORNECEDOR 
		,B.DTA_INI	
		,B.DTA_FIM
		,RANK() OVER(PARTITION BY A.COD_PRODUTO ORDER BY CONVERT(DATETIME,DTA_INI) DESC) AS SEQ1
		,RANK() OVER(PARTITION BY A.COD_PRODUTO ORDER BY CONVERT(DATETIME,DTA_GRAVACAO) DESC) AS SEQ2
		,B.VLR_EMB_COMPRA	
		,B.QTD_EMB_COMPRA
		,CASE WHEN ISNULL(B.QTD_EMB_COMPRA,1) = 0 THEN ISNULL(B.VLR_EMB_COMPRA,0) ELSE ISNULL(B.VLR_EMB_COMPRA,0)/ISNULL(B.QTD_EMB_COMPRA,1) END AS VLR_UNITARIO
FROM 
		[192.168.0.13].BI.DBO.BI_CAD_PRODUTO A
			INNER JOIN [192.168.0.13].BI.DBO.BI_PRECO_COMPRA B
				ON (B.COD_PRODUTO = A.COD_PRODUTO AND B.COD_FORNECEDOR = A.COD_FORNECEDOR)
WHERE 1=1 
		
		AND A.COD_PRODUTO IN (SELECT DISTINCT COD_PRODUTO FROM ZEUS_RTG.DBO.TAB_AJUSTE_ESTOQUE WITH (NOLOCK) WHERE 1=1 AND COD_LOJA = 33 AND CONVERT(DATE,DTA_AJUSTE) > '2016-08-01')
		AND B.COD_PRODUTO IS NOT NULL
		AND B.VLR_EMB_COMPRA > 0
) AS FIXOS
WHERE 1 = 1
		AND SEQ1 = 1
		AND SEQ2 = 1

-------------------------------------------------------------------------------------------------------
--BUSCANDO CUSTO TABELA DO FORNECEDOR DOS ITENS SEM CUSTO NO FORNECEDOR PRINCIPAL
-------------------------------------------------------------------------------------------------------

INSERT INTO #TMP_CUSTOS
SELECT 33 AS COD_LOJA , COD_PRODUTO , 0 as COD_FORNECEDOR , VLR_EMB_COMPRA , QTD_EMB_COMPRA, VLR_UNITARIO
FROM
(
SELECT  B.COD_PRODUTO
		,B.DTA_INI	
		,B.DTA_FIM
		,DTA_GRAVACAO
		,RANK() OVER(PARTITION BY b.COD_PRODUTO ORDER BY CONVERT(DATETIME,DTA_INI) DESC) AS SEQ1
		,RANK() OVER(PARTITION BY b.COD_PRODUTO ORDER BY CONVERT(DATETIME,DTA_GRAVACAO) DESC) AS SEQ2
		,B.VLR_EMB_COMPRA	
		,B.QTD_EMB_COMPRA
		,CASE WHEN ISNULL(B.QTD_EMB_COMPRA,1) = 0 THEN ISNULL(B.VLR_EMB_COMPRA,0) ELSE ISNULL(B.VLR_EMB_COMPRA,0)/ISNULL(B.QTD_EMB_COMPRA,1) END AS VLR_UNITARIO
FROM 
		[192.168.0.13].BI.DBO.BI_PRECO_COMPRA B
WHERE 1=1 
		AND B.COD_PRODUTO IN (SELECT DISTINCT COD_PRODUTO FROM ZEUS_RTG.DBO.TAB_AJUSTE_ESTOQUE WITH (NOLOCK) WHERE 1=1 AND COD_LOJA = 33 AND CONVERT(DATE,DTA_AJUSTE) > '2016-08-01') 
		AND B.COD_PRODUTO NOT IN (SELECT DISTINCT COD_PRODUTO FROM #TMP_CUSTOS) 
		AND B.VLR_EMB_COMPRA > 0

) AS FIXOS
WHERE 1=1
AND SEQ1 = 1

-------------------------------------------------------------------------------------------------------
--APLICANDO O CUSTO DEFINDO 
-------------------------------------------------------------------------------------------------------

--SELECT DISTINCT
--		A.COD_PRODUTO
--		,CAST(A.DTA_AJUSTE AS DATE) DTA_AJUSTE
--		,VAL_CUSTO_REP 
--		,CAST(CUSTO_ENTRADA AS DOUBLE PRECISION) '[DW].[DBO].PRODUTO_CUSTO'
--		,CUSTO_ULTIMA_ZEUS
--		,VLR_UNITARIO 'BI.DBO.BI_PRECO_COMPRA' 
--		,CAST(CUSTO_ENTRADA AS DOUBLE PRECISION) / VLR_UNITARIO -1 '%'
--		,CASE WHEN CAST(CUSTO_ENTRADA AS DOUBLE PRECISION) / VLR_UNITARIO -1 < 0.25 THEN CAST(CUSTO_ENTRADA AS DOUBLE PRECISION)  ELSE (CASE WHEN CUSTO_ULTIMA_ZEUS IS NULL THEN VLR_UNITARIO ELSE CUSTO_ULTIMA_ZEUS END) END AS CUSTO_EFETIVO
--		,VAL_CUSTO_REP - CASE WHEN CAST(CUSTO_ENTRADA AS DOUBLE PRECISION) / VLR_UNITARIO -1 < 0.25 THEN CAST(CUSTO_ENTRADA AS DOUBLE PRECISION)  ELSE (CASE WHEN CUSTO_ULTIMA_ZEUS IS NULL THEN VLR_UNITARIO ELSE CUSTO_ULTIMA_ZEUS END) END DIF

UPDATE A
SET 
		VAL_CUSTO_SEM_ICMS = VAL_CUSTO_REP
		,VAL_CUSTO_REP = CASE WHEN CAST(CUSTO_ENTRADA AS DOUBLE PRECISION) / VLR_UNITARIO -1 < 0.25 THEN CAST(CUSTO_ENTRADA AS DOUBLE PRECISION)  ELSE (CASE WHEN CUSTO_ULTIMA_ZEUS IS NULL THEN VLR_UNITARIO ELSE CUSTO_ULTIMA_ZEUS END) END 
FROM ZEUS_RTG.DBO.TAB_AJUSTE_ESTOQUE A WITH (NOLOCK)
		LEFT JOIN #TMP_CUSTO_DW B
				ON B.COD_LOJA = A.COD_LOJA AND CAST( B.COD_PRODUTO AS DOUBLE PRECISION) =  CAST(A.COD_PRODUTO AS DOUBLE PRECISION) 
		LEFT JOIN #TMP_CUSTOS CUSTOS
				ON CUSTOS.COD_LOJA = A.COD_LOJA AND CAST( CUSTOS.COD_PRODUTO AS DOUBLE PRECISION) =  CAST(A.COD_PRODUTO AS DOUBLE PRECISION) 
WHERE 1=1
		AND CAST(A.DTA_AJUSTE AS DATE) >= CONVERT(DATE,GETDATE()-1)
		AND A.COD_LOJA = 33
		AND (VAL_CUSTO_REP - CASE WHEN CAST(CUSTO_ENTRADA AS DOUBLE PRECISION) / VLR_UNITARIO -1 < 0.25 THEN CAST(CUSTO_ENTRADA AS DOUBLE PRECISION)  ELSE (CASE WHEN CUSTO_ULTIMA_ZEUS IS NULL THEN VLR_UNITARIO ELSE CUSTO_ULTIMA_ZEUS END) END)  <> 0

-------------------------------------------------------------------------------------------------------
--APLICANDO O CUSTO DEFINDO EM PRODUTOS COM CUSTO ZERO
-------------------------------------------------------------------------------------------------------

UPDATE A
SET 
		VAL_CUSTO_SEM_ICMS = VAL_CUSTO_REP
		,VAL_CUSTO_REP = CASE WHEN CAST(CUSTO_ENTRADA AS DOUBLE PRECISION) / VLR_UNITARIO -1 < 0.25 THEN CAST(CUSTO_ENTRADA AS DOUBLE PRECISION)  ELSE (CASE WHEN CUSTO_ULTIMA_ZEUS IS NULL THEN VLR_UNITARIO ELSE CUSTO_ULTIMA_ZEUS END) END 
FROM ZEUS_RTG.DBO.TAB_AJUSTE_ESTOQUE A WITH (NOLOCK)
		LEFT JOIN #TMP_CUSTO_DW B
				ON B.COD_LOJA = A.COD_LOJA AND CAST( B.COD_PRODUTO AS DOUBLE PRECISION) =  CAST(A.COD_PRODUTO AS DOUBLE PRECISION) 
		LEFT JOIN #TMP_CUSTOS CUSTOS
				ON CUSTOS.COD_LOJA = A.COD_LOJA AND CAST( CUSTOS.COD_PRODUTO AS DOUBLE PRECISION) =  CAST(A.COD_PRODUTO AS DOUBLE PRECISION) 
WHERE 1=1
		AND CAST(A.DTA_AJUSTE AS DATE) >= '2016-08-05'
		AND A.COD_LOJA = 33
		AND VAL_CUSTO_REP = 0
SELECT 1

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{}];
	r
];
r20\[LeftArrow]getCorrigeCUstos[$lojas];


getSTATUS[codLoja_,NotasAjustadas_]:=Module[{sql,r,conn=marcheConn[],tab},

sql=" 	

SET NOCOUNT ON;	
		
DECLARE @DTA_INI DATE = '2016-04-04' --DATA DE INICIO DO PROCESSO AUTOMATICO
DECLARE @DTA_FIM DATE = CONVERT(DATE,GETDATE()-1)

IF OBJECT_ID('tempdb..#TMP_VENDAS_VINHOS') IS NOT NULL				BEGIN 	DROP TABLE #TMP_VENDAS_VINHOS END
IF OBJECT_ID('tempdb..#TMP_VENDA_GARRAFAS') IS NOT NULL				BEGIN  	DROP TABLE #TMP_VENDA_GARRAFAS END
IF OBJECT_ID('tempdb..#TAB_TRANSF') IS NOT NULL						BEGIN	DROP TABLE #TAB_TRANSF END
IF OBJECT_ID('tempdb..#TAB_ESTORNO') IS NOT NULL					BEGIN 	DROP TABLE #TAB_ESTORNO END
IF OBJECT_ID('tempdb..#TMP_VENDA_OUTROS') IS NOT NULL				BEGIN	DROP TABLE #TMP_VENDA_OUTROS END
IF OBJECT_ID('tempdb..#TMP_RESULT') IS NOT NULL						BEGIN   DROP TABLE #TMP_RESULT END
IF object_id('tempdb..#TMP_TAB_AJUSTE_ESTOQUE') IS NOT NULL			BEGIN   DROP TABLE #TMP_TAB_AJUSTE_ESTOQUE END
IF object_id('tempdb..#TMP_VALIDACAO') IS NOT NULL					BEGIN   DROP TABLE #TMP_VALIDACAO END

CREATE TABLE [#TMP_VALIDACAO]

	(		[Order] INT
			,Validacao varchar(max)
			,Status varchar(max)
			,Qtde Int
			,Dta date
			,Responsavel Varchar(max)
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
		AND COD_LOJA IN (`1`)
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
		AND COD_LOJA IN (`1`)
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
		AND COD_AJUSTE IN (281)
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
		AND COD_LOJA IN (`1`)
		AND CONVERT(DATE,DATA) >= @DTA_INI
		AND CONVERT(DATE,DATA) <= @DTA_FIM
		AND COD_DEPARTAMENTO NOT IN (2)
		AND A.COD_PRODUTO IN (SELECT DISTINCT V.COD_PRODUTO FROM  [192.168.0.13].[BI].[DBO].BI_VENDA_PRODUTO V  with (nolock) INNER JOIN [ZEUS_RTG].[DBO].[VW_MARCHE_ENTRADAS] E ON (E.COD_LOJA = V.COD_LOJA AND E.COD_PRODUTO = V.COD_PRODUTO) WHERE 1=1 AND V.COD_LOJA = 29 AND E.COD_LOJA = 29 AND V.COD_PRODUTO NOT IN (1019239,1020263,1027074))	 
GROUP BY
		A.COD_LOJA
	   ,A.COD_PRODUTO				
----------------------------------------------------------------------
--INSERINDO DADOS NA AUDITORIA EATALYL TRANSFER\[CapitalEHat]NCIAS AUTOMATICA NA GRID
----------------------------------------------------------------------	
insert into [#TMP_VALIDACAO] ([Order], Validacao, Status, Qtde , Dta, Responsavel)		 

SELECT 
		1
		,'EatalyL Transfer\[EHat]ncias Automatica' Validacao 
		,Case when  COUNT(VENDAS.COD_PRODUTO) > 0 then 'Pendente' else 'Ok' end Status
		,COUNT(VENDAS.COD_PRODUTO) Qtde
		,Case when  COUNT(VENDAS.COD_PRODUTO) > 0 then  getdate() else null end DATA
		,'Diego Miller'
FROM #TMP_VENDA_OUTROS VENDAS 
		LEFT JOIN #TAB_TRANSF TRANSF 
				ON TRANSF.COD_PRODUTO = VENDAS.COD_PRODUTO
		LEFT JOIN #TAB_ESTORNO ESTORNO
				ON ESTORNO.COD_PRODUTO = VENDAS.COD_PRODUTO
		LEFT JOIN  [192.168.0.13].BI.DBO.BI_CAD_PRODUTO C  with (nolock)
				ON C.COD_PRODUTO = VENDAS.COD_PRODUTO 
WHERE 1=1
		AND QTD_VENDA-(ISNULL(QTD_TRANSF,0)+ISNULL(QTD_ESTORNO,0)) > 0 		
		AND VENDAS.COD_PRODUTO NOT IN (1019239,1020263,1027074,01030730,01030731,00049566)				
----------------------------------------------------------------------
--CHECK EMISSAO DE NF
----------------------------------------------------------------------	
IF OBJECT_ID('tempdb..#TMP_TRANSF') IS NOT NULL		BEGIN		DROP TABLE #TMP_TRANSF END
IF OBJECT_ID('tempdb..#TAB_EMITIDA') IS NOT NULL	   BEGIN		DROP TABLE #TAB_EMITIDA END
IF OBJECT_ID('tempdb..#TMP_CUSTO') IS NOT NULL		 BEGIN		DROP TABLE #TMP_CUSTO END
IF OBJECT_ID('tempdb..#TAB_NFD') IS NOT NULL		   BEGIN		DROP TABLE #TAB_NFD END
IF OBJECT_ID('tempdb..#TMP_EMISSAO') IS NOT NULL	   BEGIN 		DROP TABLE #TMP_EMISSAO END
----------------------------------------------------------------------			
--BUSCANDO TRANSFENRENCIAS
----------------------------------------------------------------------					
SELECT 
		COD_LOJA
		,COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_TRANSF
		,SUM((QTD_AJUSTE*VAL_CUSTO_REP)*-1) AS CUSTO_TRANSF
		INTO #TMP_TRANSF
FROM TAB_AJUSTE_ESTOQUE  WITH (NOLOCK) 
		WHERE 1=1
		AND COD_AJUSTE IN (269,281,13)
		AND COD_LOJA = 29
GROUP BY 
		COD_LOJA
		,COD_PRODUTO
----------------------------------------------------------------------			
--VENDAS NF EMISSAO--
----------------------------------------------------------------------		
SELECT 
		COD_PRODUTO
		,SUM(CAST((QTD_EMBALAGEM*QTD_ENTRADA) AS DOUBLE PRECISION)) AS QTD_EMITIDA
INTO #TAB_EMITIDA
FROM [ZEUS_RTG].[DBO].[VW_NF_SAIDA]
WHERE 1=1
		AND COD_LOJA IN (29)
		AND COD_CLIENTE = 1960502441		
		AND COD_FISCAL = 192	
GROUP BY 
		COD_PRODUTO
----------------------------------------------------------------------			
--BUSCANDO NFD'S
----------------------------------------------------------------------			
SELECT 
		COD_PRODUTO*1 COD_PRODUTO
		,SUM(CAST((QTD_EMBALAGEM*QTD_ENTRADA) AS DOUBLE PRECISION)) AS QTD_DEVOLVIDA
INTO #TAB_NFD
FROM [192.168.0.6].[ZEUS_RTG].[DBO].VW_NF_DEVOLUCAO
WHERE 1=1
		AND COD_LOJA IN (`1`)
		AND COD_FORNECEDOR = 102856
GROUP BY 
		COD_PRODUTO
----------------------------------------------------------------------			
--INSERINDO DADOS NA AUDITORIA EATALYL EMISS\[CapitalATilde]O NF AUTOMATICA NA GRID
----------------------------------------------------------------------			
insert into  [#TMP_VALIDACAO] ([Order], Validacao, Status, Qtde , Dta, Responsavel)	
SELECT  
		2
		,'EatalyL Emiss\[ATilde]o NF Automatica' Validacao 
		,Case when  COUNT(TRANSF.COD_PRODUTO) > 0 then 'Pendente' else 'Ok' end Status
		,COUNT(TRANSF.COD_PRODUTO) Qtde 
		,Case when  COUNT(TRANSF.COD_PRODUTO) > 0 then  getdate() else null end DATA
		,'Diego Miller'
FROM
#TMP_TRANSF AS TRANSF 
        LEFT JOIN #TAB_ESTORNO ESTORNO
				ON ESTORNO.COD_PRODUTO = TRANSF.COD_PRODUTO
		LEFT JOIN #TAB_EMITIDA EMITIDA
				ON EMITIDA.COD_PRODUTO = TRANSF.COD_PRODUTO
		LEFT JOIN #TAB_NFD NFD
				ON NFD.COD_PRODUTO = TRANSF.COD_PRODUTO
		LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO AS CAD 
				ON  CAD.COD_PRODUTO = TRANSF.COD_PRODUTO 
WHERE 1=1 
		AND (QTD_TRANSF + ISNULL(QTD_ESTORNO,0)) - (ISNULL(QTD_EMITIDA,0) + ISNULL(QTD_DEVOLVIDA,0)*-1)  > 0.01
----------------------------------------------------------------------
-- CHECK EMISSAO ENTRADAS PENDENTE INTERCOMPANY
---------------------------------------------------------------------- 
IF OBJECT_ID('tempdb..#TEMP_CADASTRO_LOJAS') IS NOT NULL	BEGIN 		DROP TABLE #TEMP_CADASTRO_LOJAS END

SELECT 
		A.COD_LOJA,
		DES_CLIENTE,
		A.NUM_CGC,
		COD_CLIENTE
INTO #TEMP_CADASTRO_LOJAS
FROM TAB_LOJA AS A 
	LEFT JOIN TAB_CLIENTE AS B
		ON A.NUM_CGC = B.NUM_CGC


insert into [#TMP_VALIDACAO] ([Order], Validacao, Status, Qtde , Dta, Responsavel)	
SELECT 
		3
		,'EatalyR Entrada Intercompany Pendente' 
		,Case when  COUNT(EMITIDA_DE) > 0 then 'Pendente' else 'Ok' end Status
		,COUNT(EMITIDA_DE) AS QTDE 
		, MIN(DTA_EMISSAO) ULT_EMISSAO 
		,'Celso'
FROM 
	(
			SELECT 
					DISTINCT SAIDA.NUM_NF_CLI
					,SAIDA.COD_LOJA AS EMITIDA_DE
					,LOJAS.COD_LOJA AS ENTRADA_PARA
					,CONVERT (DATE,SAIDA.DTA_EMISSAO)AS DTA_EMISSAO
					,(CASE WHEN ENTRADAS.COD_PRODUTO <> 0  THEN 'OK'  ELSE 'PENDENTE' END) AS VALIDACAO
			FROM VW_NF_SAIDA AS SAIDA with (NOLOCK) 
					LEFT JOIN #TEMP_CADASTRO_LOJAS AS LOJAS 
						ON LOJAS.COD_CLIENTE = SAIDA.COD_CLIENTE
					LEFT JOIN VW_MARCHE_ENTRADAS AS ENTRADAS
						ON 1=1
						AND ENTRADAS.COD_LOJA = LOJAS.COD_LOJA 
						AND ENTRADAS.NUM_NF_FORN = SAIDA.NUM_NF_CLI 
						AND SAIDA.COD_PRODUTO = ENTRADAS.COD_PRODUTO 
			WHERE 1=1

					AND LOJAS.COD_LOJA IS NOT NULL 
					AND ENTRADAS.COD_PRODUTO IS NULL
					AND CONVERT (DATE,SAIDA.DTA_EMISSAO) >= '2016-08-05'
					AND LOJAS.COD_LOJA IN (`1`)
					AND SAIDA.NUM_NF_CLI NOT IN (`2`)

	) TMP

	GROUP BY 
				ENTRADA_PARA

----------------------------------------------------------------------
-- CHECK EATALYR TRANSFER\[CapitalEHat]NCIAS PENDENTES DE APROVA\[CapitalCCedilla]\[CapitalATilde]O
---------------------------------------------------------------------- 

insert into [#TMP_VALIDACAO] ([Order], Validacao, Status, Qtde , Dta, Responsavel)	
SELECT 
		5
		,'EatalyR Transfer\[EHat]ncias Pendentes de Aprova\[CCedilla]\[ATilde]o'
		,Case when  COUNT(*)  > 0 then 'Pendente' else 'Ok' end Status
		,COUNT(DISTINCT T.ID) AS qtde
		,MIN(CONVERT(DATE,DATA))
		,'Diego Miller'
FROM [INTRANET].[DBO].[TAB_TRANSFINTERNAS] t
WHERE 1=1
		AND T.COD_LOJA IN (`1`)
		AND T.STATUS = 6
		AND convert(date,DATA) >= '2016-08-05'-- CONVERT(DATE,GETDATE())

----------------------------------------------------------------------
-- CHECK EATALYR MOVIMENTA\[CapitalCCedilla]\[CapitalOTilde]ES SEM CUSTO
---------------------------------------------------------------------- 

insert into [#TMP_VALIDACAO] ([Order], Validacao, Status, Qtde , Dta, Responsavel)	
SELECT 
		6
		,'EatalyR Movimenta\[CCedilla]\[OTilde]es Sem Custo'
		,Case when COUNT(DISTINCT COD_AJUSTE_ESTOQUE) > 0 then 'Pendente' else 'Ok' end Status
		,COUNT(DISTINCT COD_AJUSTE_ESTOQUE) AS Qtde 
		,MIN(CONVERT(DATE,DTA_AJUSTE))
		,'Diego Miller' 
FROM ZEUS_RTG.DBO.TAB_AJUSTE_ESTOQUE A
	
WHERE 1=1
		AND ISNULL(VAL_CUSTO_REP,0) = 0 
		AND	A.COD_LOJA IN (`1`)
		AND DTA_AJUSTE >= '2016-08-01'

----------------------------------------------------------------------
-- CHECK EATALYR MOVIMENTA\[CapitalCCedilla]\[CapitalOTilde]ES SEM CUSTO
---------------------------------------------------------------------- 

IF OBJECT_ID('tempdb..#TMP_NFE_STATUS') IS NOT NULL		BEGIN		DROP TABLE #TMP_NFE_STATUS END
IF OBJECT_ID('tempdb..#TMP_ZEUS') IS NOT NULL			BEGIN		DROP TABLE #TMP_ZEUS END

SELECT 
		 NUM_DANFE DANFE
		,FLG_RECEBIMENTO
		,FLG_ENTRADAAUTOMATICA
		,FLG_PREDANFE
		,FLG_ENTRADAZEUS
		,DT_ENTRADAZEUS
		,COD_FORNECEDOR
		,CGCCPF NUN_CGC
		,UPPER(FORNECEDOR) FORNECEDOR
		,CAST (NF AS DOUBLE PRECISION)	AS NUM_NF
		,LEFT (NF*1,6)					AS NF_NUMERO_DIREITA 
		,RIGHT (NF*1,6)					AS NF_NUMERO_ESQUERDA 
		,COD_LOJA
		,DTRECEBIMENTO
		,FLG_CANCELADO_FORNECEDOR
INTO #TMP_NFE_STATUS
FROM [192.168.0.13].[CTRLNFE].[DBO].NFE_STATUS
WHERE 1=1 
		AND COD_LOJA IN (`1`)
		AND FLG_CANCELADO_FORNECEDOR IS NULL
		AND FLG_RECEBIMENTO = 1
		AND COD_FORNECEDOR NOT IN (102856,103952)
		AND CONVERT(DATE,DTRECEBIMENTO) >= '2016-08-05'
-----------------------------------------------------------------------------------------------------------
-- BUSCANDO ENTRADAS ZEUS
-----------------------------------------------------------------------------------------------------------		
		SELECT   COD_LOJA
				,A.COD_FORNECEDOR
				,NUM_CGC
				,NUM_NF_FORN
				,CONVERT(DATE,DTA_EMISSAO) DTA_EMISSAO
				,CAST(NUM_DANFE AS VARCHAR(44)) NUM_DANFE
				,VAL_TOTAL_NF
		INTO #TMP_ZEUS 
		FROM [ZEUS_RTG].[DBO].TAB_FORNECEDOR_NOTA A WITH (NOLOCK)
				LEFT JOIN [ZEUS_RTG].[DBO].TAB_FORNECEDOR B  WITH (NOLOCK)
						ON B.COD_FORNECEDOR = A.COD_FORNECEDOR
		WHERE 1=1
				AND CONVERT(DATE,DTA_ENTRADA) >= '2015-01-01' 
				AND DES_ESPECIE IN ('NF','DAN')
				AND COD_LOJA IN (`1`)
-----------------------------------------------------------------------------------------------------------
-- LIMPANDO NOTAS ENCONTRADAS LANCADAS NO ZEUS
-----------------------------------------------------------------------------------------------------------		
		DELETE A
		FROM #TMP_NFE_STATUS A 
				LEFT JOIN #TMP_ZEUS B 
						ON CAST(B.NUM_DANFE AS VARCHAR(44)) =  CAST(A.DANFE AS VARCHAR(44)) COLLATE SQL_Latin1_General_CP1_CI_AS AND B.COD_LOJA = A.COD_LOJA
		WHERE 1=1  
				AND B.COD_LOJA IS NOT NULL  

		DELETE A
		FROM #TMP_NFE_STATUS AS A 
			LEFT JOIN #TMP_ZEUS AS B 
				ON B.COD_LOJA = A.COD_LOJA 
				AND CAST(B.NUM_CGC AS DOUBLE PRECISION) = CAST(A.NUN_CGC AS DOUBLE PRECISION)
				AND CAST(B.NUM_NF_FORN AS DOUBLE PRECISION) = CAST(A. NUM_NF AS DOUBLE PRECISION)
		WHERE 1=1 
				AND B.COD_LOJA IS NOT NULL	

		DELETE A
		FROM #TMP_NFE_STATUS AS A 
			LEFT JOIN #TMP_ZEUS AS B 
				ON B.COD_LOJA = A.COD_LOJA 
				AND CAST(B.NUM_CGC AS DOUBLE PRECISION) = CAST(A.NUN_CGC AS DOUBLE PRECISION)
				AND CAST(B.NUM_NF_FORN AS DOUBLE PRECISION) = CAST(A.NF_NUMERO_ESQUERDA AS DOUBLE PRECISION)
		WHERE 1=1 
				AND B.COD_LOJA IS NOT NULL	
				
		DELETE A
		FROM #TMP_NFE_STATUS AS A 
			LEFT JOIN #TMP_ZEUS AS B 
				ON B.COD_LOJA = A.COD_LOJA 
				AND CAST(B.NUM_CGC AS DOUBLE PRECISION) = CAST(A.NUN_CGC AS DOUBLE PRECISION)
				AND CAST(B.NUM_NF_FORN AS DOUBLE PRECISION) = CAST(A.NF_NUMERO_DIREITA AS DOUBLE PRECISION)
		WHERE 1=1 
				AND B.COD_LOJA IS NOT NULL	
-----------------------------------------------------------------------------------------------------------
--TENTANDO LAN\[CapitalCCedilla]AMENTO AUTOMATICO
-----------------------------------------------------------------------------------------------------------
IF EXISTS (SELECT TOP 1 1 FROM #TMP_NFE_STATUS)
	BEGIN
						DECLARE DB_Cursor_NFs CURSOR FOR  
						SELECT  DANFE FROM #TMP_NFE_STATUS
						DECLARE @DANFE VARCHAR(44)
				
						OPEN DB_Cursor_NFs   
						FETCH NEXT FROM DB_Cursor_NFs INTO @DANFE

						WHILE @@FETCH_STATUS = 0   
						BEGIN   

							EXEC [192.168.0.13].INTEGRACOES.DBO.CTRLNFE_ENTRADA_AUTOMATICA_60 @DANFE
						
						FETCH NEXT FROM DB_Cursor_NFs INTO @DANFE
						END   

						CLOSE DB_Cursor_NFs   
						DEALLOCATE DB_Cursor_NFs

	END
-----------------------------------------------------------------------------------------------------------
-- FAZENDO UMA NOVA BUSCA NO ZEUS
-----------------------------------------------------------------------------------------------------------	
		TRUNCATE TABLE #TMP_ZEUS

		INSERT INTO #TMP_ZEUS
		SELECT   COD_LOJA
				,A.COD_FORNECEDOR
				,NUM_CGC
				,NUM_NF_FORN
				,CONVERT(DATE,DTA_EMISSAO) DTA_EMISSAO
				,CAST(NUM_DANFE AS VARCHAR(44)) NUM_DANFE
				,VAL_TOTAL_NF
		FROM [ZEUS_RTG].[DBO].TAB_FORNECEDOR_NOTA A WITH (NOLOCK)
				LEFT JOIN [ZEUS_RTG].[DBO].TAB_FORNECEDOR B  WITH (NOLOCK)
						ON B.COD_FORNECEDOR = A.COD_FORNECEDOR
		WHERE 1=1
				AND CONVERT(DATE,DTA_ENTRADA) >= '2015-01-01' 
				AND DES_ESPECIE IN ('NF','DAN')
				AND COD_LOJA IN (`1`)	
-----------------------------------------------------------------------------------------------------------
-- LIMPANDO NOTAS ENCONTRADAS LANCADAS NO ZEUS
-----------------------------------------------------------------------------------------------------------		
		DELETE A
		FROM #TMP_NFE_STATUS A 
				LEFT JOIN #TMP_ZEUS B 
						ON CAST(B.NUM_DANFE AS VARCHAR(44)) =  CAST(A.DANFE AS VARCHAR(44)) COLLATE SQL_Latin1_General_CP1_CI_AS AND B.COD_LOJA = A.COD_LOJA
		WHERE 1=1  
				AND B.COD_LOJA IS NOT NULL  

		DELETE A
		FROM #TMP_NFE_STATUS AS A 
			LEFT JOIN #TMP_ZEUS AS B 
				ON B.COD_LOJA = A.COD_LOJA 
				AND CAST(B.NUM_CGC AS DOUBLE PRECISION) = CAST(A.NUN_CGC AS DOUBLE PRECISION)
				AND CAST(B.NUM_NF_FORN AS DOUBLE PRECISION) = CAST(A. NUM_NF AS DOUBLE PRECISION)
		WHERE 1=1 
				AND B.COD_LOJA IS NOT NULL	

		DELETE A
		FROM #TMP_NFE_STATUS AS A 
			LEFT JOIN #TMP_ZEUS AS B 
				ON B.COD_LOJA = A.COD_LOJA 
				AND CAST(B.NUM_CGC AS DOUBLE PRECISION) = CAST(A.NUN_CGC AS DOUBLE PRECISION)
				AND CAST(B.NUM_NF_FORN AS DOUBLE PRECISION) = CAST(A.NF_NUMERO_ESQUERDA AS DOUBLE PRECISION)
		WHERE 1=1 
				AND B.COD_LOJA IS NOT NULL	
				
		DELETE A
		FROM #TMP_NFE_STATUS AS A 
			LEFT JOIN #TMP_ZEUS AS B 
				ON B.COD_LOJA = A.COD_LOJA 
				AND CAST(B.NUM_CGC AS DOUBLE PRECISION) = CAST(A.NUN_CGC AS DOUBLE PRECISION)
				AND CAST(B.NUM_NF_FORN AS DOUBLE PRECISION) = CAST(A.NF_NUMERO_DIREITA AS DOUBLE PRECISION)
		WHERE 1=1 
				AND B.COD_LOJA IS NOT NULL	
-----------------------------------------------------------------------------------------------------------
--INSERINDO DADOS NA AUDITORIA EATALYR ENTRADA PENDENTE FORN NA GRID
-----------------------------------------------------------------------------------------------------------
insert into [#TMP_VALIDACAO] ([Order], Validacao, Status, Qtde , Dta, Responsavel)	
SELECT 
		4
		,'EatalyR Entrada Pendente Forn'
		,Case when count(*) > 0 then 'Pendente' else 'Ok' end Status
		,COUNT(*) Qtde
		,MIN(convert(date,DTRECEBIMENTO)) DTA
		,'Celso'
FROM #TMP_NFE_STATUS

SELECT
		Validacao as [Valida\[CCedilla]\[ATilde]o]
		,Status
		,Qtde
		,convert(varchar,Dta,103) Data
		,Responsavel 
FROM [#TMP_VALIDACAO] 

ORDER BY [Order]

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLArgument@@NotasAjustadas,}];
	r
];
r4\[LeftArrow]getSTATUS[$lojas,$NotasAjustadas];


gridStatus[data_Symbol]:=Module[{grid,title,r,color2,stl,total},
	r\[LeftArrow]data;

	 
(*
  total={"Total",Total@r[[All,"Custo Estoque"]],Total@r[[All,"Qtde Produtos"]],Total@r[[All,"Produtos Custo Zero"]]};
       r["DataAll"]=Join[r["DataAll"],{total}];
*)	
	color1=Which[ 
	 #>0,Red
	,True,Darker@Green
	]&;

    stl[val_]:=Style[maF["N"][val],Bold,color1@val];
	stl[Null]="0"; 

	color2=Which[ 
	 #>7,Black
	,True,Darker@Green
	]&;

    stl2[val_]:=Style[maF["N,00"][val],color2@val];
	stl2[Null]="0"; 

	grid=maReportGrid[r,"ColumnFormat"->{{"Qtde"}-> stl},"TotalRow"->False];
	title=Style[Row@{"Movimenta\[CCedilla]\[OTilde]es"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
gridStatus[r4];


getEstoqueExcel[]:=Module[{sql,r,conn=marcheConn2[],tab},
sql=" 
	
SET NOCOUNT ON;	

SELECT * INTO #METADADOS FROM [BI].[DBO].CADASTRO_CAD_PRODUTO_METADADOS WHERE 1=1 AND COD_METADADO = 47

SELECT
		E.COD_LOJA
		,E.COD_PRODUTO
		,DESCRICAO
		,NO_DEPARTAMENTO
		,NO_SECAO
		,NO_GRUPO
		,NO_SUBGRUPO
		,NO_CLASSIF	
		,NO_SUB_CLASSIF
		,CASE WHEN COD_METADADO = 47 AND VLR_METADADO = 1 THEN VLR_METADADO ELSE 0 END 'PI'
		,CAST(QTD_ESTOQUE AS DOUBLE PRECISION) QTD_ESTOQUE
		,QTD_ESTOQUE*ISNULL(CAST(CUSTO AS DOUBLE PRECISION),0) CUSTO
		,CASE WHEN ISNULL(CUSTO,0) = 0 THEN 1 ELSE 0 END CUSTO_ZERO

FROM DW.DBO.ESTOQUE E
		LEFT JOIN DW.DBO.PRODUTO_CUSTOS CUSTO
				ON CUSTO.COD_LOJA = E.COD_LOJA AND CUSTO.COD_PRODUTO = E.COD_PRODUTO
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO CAD
				ON CAD.COD_PRODUTO = E.COD_PRODUTO
		LEFT JOIN BI.dbo.EAT_DEPARA_CLASSIF_DASH CLASS
				ON 1=1
				AND CLASS.COD_DEPARTAMENTO = CAD.COD_DEPARTAMENTO	
				AND CLASS.COD_SECAO = CAD.COD_SECAO	
				AND CLASS.COD_GRUPO = CAD.COD_GRUPO	
				AND CLASS.COD_SUB_GRUPO = CAD.COD_SUB_GRUPO
		LEFT JOIN #METADADOS META
				ON META.COD_PRODUTO = E.COD_PRODUTO	

WHERE 1=1
		AND E.COD_LOJA = 33 
		AND QTD_ESTOQUE <> 0
		AND CONVERT(DATE,DATA) = CONVERT(DATE,GETDATE())
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql(*,{SQLDateTime@@DtaIni}*)];
	r
];

(*r2\[LeftArrow]getEstoqueExcel[]*)


getNFPendentesExcel[NotasAjustadas_]:=Module[{sql,r,conn=marcheConn[],tab},
sql=" 

SET NOCOUNT ON;	

IF OBJECT_ID('tempdb..#TEMP_CADASTRO_LOJAS') IS NOT NULL	BEGIN 		DROP TABLE #TEMP_CADASTRO_LOJAS END

SELECT 
		A.COD_LOJA,
		DES_CLIENTE,
		A.NUM_CGC,
		COD_CLIENTE
INTO #TEMP_CADASTRO_LOJAS
FROM TAB_LOJA AS A 
	LEFT JOIN TAB_CLIENTE AS B
		ON A.NUM_CGC = B.NUM_CGC


IF EXISTS (

SELECT 
DISTINCT TOP 1 1 FROM VW_NF_SAIDA AS SAIDA WITH (NOLOCK) 
	LEFT JOIN #TEMP_CADASTRO_LOJAS AS LOJAS 
		ON LOJAS.COD_CLIENTE = SAIDA.COD_CLIENTE
	LEFT JOIN VW_MARCHE_ENTRADAS AS ENTRADAS 
		ON 1=1
		AND ENTRADAS.COD_LOJA = LOJAS.COD_LOJA 
		AND ENTRADAS.NUM_NF_FORN = SAIDA.NUM_NF_CLI 
		AND SAIDA.COD_PRODUTO = ENTRADAS.COD_PRODUTO 
WHERE 1=1
		AND LOJAS.COD_LOJA IS NOT NULL 
		AND ENTRADAS.COD_PRODUTO IS NULL
		AND CONVERT (DATE,SAIDA.DTA_EMISSAO) >= '2016-08-05'
		AND LOJAS.COD_LOJA = 33
		AND SAIDA.NUM_NF_CLI NOT IN (`1`)

		)

			BEGIN 

					SELECT 
							DISTINCT SAIDA.NUM_NF_CLI
							,SAIDA.COD_LOJA AS EMITIDA_DE
							,LOJAS.COD_LOJA AS ENTRADA_PARA
							,CONVERT (DATE,SAIDA.DTA_EMISSAO)AS DTA_EMISSAO
							,(CASE WHEN ENTRADAS.COD_PRODUTO <> 0  THEN 'OK'  ELSE 'PENDENTE' END) AS VALIDACAO
							FROM VW_NF_SAIDA AS SAIDA with (NOLOCK) 
								LEFT JOIN #TEMP_CADASTRO_LOJAS AS LOJAS 
									ON LOJAS.COD_CLIENTE = SAIDA.COD_CLIENTE
								LEFT JOIN VW_MARCHE_ENTRADAS AS ENTRADAS 
									ON 1=1
									AND ENTRADAS.COD_LOJA = LOJAS.COD_LOJA 
									AND ENTRADAS.NUM_NF_FORN = SAIDA.NUM_NF_CLI 
									AND SAIDA.COD_PRODUTO = ENTRADAS.COD_PRODUTO 
					WHERE 1=1
							AND LOJAS.COD_LOJA IS NOT NULL 
							AND ENTRADAS.COD_PRODUTO IS NULL
							AND CONVERT (DATE,SAIDA.DTA_EMISSAO) >= '2016-08-05'
							AND LOJAS.COD_LOJA = 33
							AND SAIDA.NUM_NF_CLI NOT IN (`1`)
			END 

				ELSE 

			BEGIN
					SELECT 0,	0,	0,	CONVERT(DATE,GETDATE()),	'OK'
			END
		
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@NotasAjustadas}];
	r
];

(*r5\[LeftArrow]getNFPendentesExcel[$NotasAjustadas]*)


getNFFornPendentesExcel[]:=Module[{sql,r,conn=marcheConn[],tab},
sql=" 

SET NOCOUNT ON;	

IF OBJECT_ID('tempdb..#TMP_NFE_STATUS') IS NOT NULL		BEGIN		DROP TABLE #TMP_NFE_STATUS END
IF OBJECT_ID('tempdb..#TMP_ZEUS') IS NOT NULL			  BEGIN		DROP TABLE #TMP_ZEUS END

SELECT   COD_LOJA
		,CAST(DTRECEBIMENTO AS DATE)  DTRECEBIMENTO
		,CAST(DT_ENTRADAZEUS AS DATE)  DTENTRADAZEUS
		,CGCCPF NUN_CGC
		,COD_FORNECEDOR
		,UPPER(FORNECEDOR) FORNECEDOR
		,CAST (NF AS DOUBLE PRECISION)	AS NUM_NF
		,LEFT (NF*1,6)					AS NF_NUMERO_DIREITA 
		,RIGHT (NF*1,6)					AS NF_NUMERO_ESQUERDA 
		,NUM_DANFE DANFE
		,ISNULL(FLG_RECEBIMENTO,0)  FLG_RECEBIMENTO
		,ISNULL(FLG_ENTRADAAUTOMATICA,0)  FLG_ENTRADAAUTOMATICA
		,ISNULL(FLG_PREDANFE,0)  FLG_PREDANFE
		,ISNULL(FLG_ENTRADAZEUS,0)  FLG_ENTRADAZEUS
		,ISNULL(FLG_CANCELADO_FORNECEDOR,0)  FLG_CANCELADO_FORNECEDOR
INTO #TMP_NFE_STATUS
FROM [192.168.0.13].[CTRLNFE].[DBO].NFE_STATUS
WHERE 1=1 
		AND COD_LOJA = 33 
		AND FLG_CANCELADO_FORNECEDOR IS NULL
		AND FLG_RECEBIMENTO = 1
		AND COD_FORNECEDOR NOT IN (102856,103952)
		AND CONVERT(DATE,DTRECEBIMENTO) >= '2016-08-05'
-----------------------------------------------------------------------------------------------------------
-- BUSCANDO ENTRADAS ZEUS
-----------------------------------------------------------------------------------------------------------		
		SELECT   COD_LOJA
				,A.COD_FORNECEDOR
				,NUM_CGC
				,NUM_NF_FORN
				,CONVERT(DATE,DTA_EMISSAO) DTA_EMISSAO
				,CAST(NUM_DANFE AS VARCHAR(44)) NUM_DANFE
				,VAL_TOTAL_NF
		INTO #TMP_ZEUS 
		FROM [ZEUS_RTG].[DBO].TAB_FORNECEDOR_NOTA A WITH (NOLOCK)
				LEFT JOIN [ZEUS_RTG].[DBO].TAB_FORNECEDOR B  WITH (NOLOCK)
						ON B.COD_FORNECEDOR = A.COD_FORNECEDOR
		WHERE 1=1
				AND CONVERT(DATE,DTA_ENTRADA) >= '2015-01-01' 
				AND DES_ESPECIE IN ('NF','DAN')
				AND COD_LOJA IN (33)
-----------------------------------------------------------------------------------------------------------
-- LIMPANDO NOTAS ENCONTRADAS LANCADAS NO ZEUS
-----------------------------------------------------------------------------------------------------------		
		DELETE A
		FROM #TMP_NFE_STATUS A 
				LEFT JOIN #TMP_ZEUS B 
						ON CAST(B.NUM_DANFE AS VARCHAR(44)) =  CAST(A.DANFE AS VARCHAR(44)) COLLATE SQL_Latin1_General_CP1_CI_AS AND B.COD_LOJA = A.COD_LOJA
		WHERE 1=1  
				AND B.COD_LOJA IS NOT NULL  

		DELETE A
		FROM #TMP_NFE_STATUS AS A 
			LEFT JOIN #TMP_ZEUS AS B 
				ON B.COD_LOJA = A.COD_LOJA 
				AND CAST(B.NUM_CGC AS DOUBLE PRECISION) = CAST(A.NUN_CGC AS DOUBLE PRECISION)
				AND CAST(B.NUM_NF_FORN AS DOUBLE PRECISION) = CAST(A. NUM_NF AS DOUBLE PRECISION)
		WHERE 1=1 
				AND B.COD_LOJA IS NOT NULL	

		DELETE A
		FROM #TMP_NFE_STATUS AS A 
			LEFT JOIN #TMP_ZEUS AS B 
				ON B.COD_LOJA = A.COD_LOJA 
				AND CAST(B.NUM_CGC AS DOUBLE PRECISION) = CAST(A.NUN_CGC AS DOUBLE PRECISION)
				AND CAST(B.NUM_NF_FORN AS DOUBLE PRECISION) = CAST(A.NF_NUMERO_ESQUERDA AS DOUBLE PRECISION)
		WHERE 1=1 
				AND B.COD_LOJA IS NOT NULL	
				
		DELETE A
		FROM #TMP_NFE_STATUS AS A 
			LEFT JOIN #TMP_ZEUS AS B 
				ON B.COD_LOJA = A.COD_LOJA 
				AND CAST(B.NUM_CGC AS DOUBLE PRECISION) = CAST(A.NUN_CGC AS DOUBLE PRECISION)
				AND CAST(B.NUM_NF_FORN AS DOUBLE PRECISION) = CAST(A.NF_NUMERO_DIREITA AS DOUBLE PRECISION)
		WHERE 1=1 
				AND B.COD_LOJA IS NOT NULL	

		SELECT * FROM #TMP_NFE_STATUS
		
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql(*{SQLArgument@@NotasAjustadas}*)];
	r
];

r55\[LeftArrow]getNFFornPendentesExcel[];


ClearAll@createXLSXReport;
createXLSXReport[]:=Module[{subject,body,tables,dates,intervalType,tempFile,destFile,r2,u,r5,r55,newFile,resp},
	
	tempFile=FileNameJoin@{mrtFileDirectory[],"Bases.xlsx"};
	$destFile=FileNameJoin@{mrtFileDirectory[],"reports","EatalyR Estoque e Pendencias.xlsx"};
	
	r2\[LeftArrow]getEstoqueExcel[];
	r5\[LeftArrow]getNFPendentesExcel[$NotasAjustadas];
	r55\[LeftArrow]getNFFornPendentesExcel[];

	Check[
		mrtUpdateExcelTemplate[
		 tempFile
		,$destFile
		,{ 
                 <|"Sheet" -> "Estoque", "TableBase" -> "TableEstoque" , "Data" -> r2["Data"]|>
				,<|"Sheet" -> "Intercompany", "TableBase" -> "TableIntercompany" , "Data" -> r5["Data"]|>
				,<|"Sheet" -> "Fornecedores", "TableBase" -> "TableForn" , "Data" -> r55["Data"]|>
		 }
		];
		,$destFile=$Failed
	];
	
	$destFile
]

createXLSXReport[];


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

createReport[data_Symbol]:=Module[{s},
	s\[LeftArrow]data;
	Grid[{{gridLojas[r1],gridStatus[r4]}},Spacings->{1.5,4},Frame->Transparent]
]
createReport[s];

rep=marcheTemplate[grid=Row@{"Status EatalyR ",$now},createReport[s],1200, $logo];
$fileName1="Status EatalyR.png";
Export["Status EatalyR.png",rep];


marcheMail[
  		"Status EatalyR " <>$now
  		,""
		  ,$mailsGerencia
		  ,{$fileName1,$destFile}	
]


(*"diego.miller@marche.com.br"*)

