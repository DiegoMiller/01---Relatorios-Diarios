(* ::Package:: *)

(* ::Section:: *)
(*Estoque Loja Vs CD*)


Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


maGetDeParaLojas[]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql="
		SELECT  [COD_LOJA]
			  ,[NO_LOJA]
		FROM [BI].[dbo].[BI_CAD_LOJA2]
	";
	r\[LeftArrow]mrtSQLDataObject[conn,sql];
	Rule@@@r["Data"]
]
maGetDeParaLojas[];


maGetDeParaDpto[]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql="
		SELECT DISTINCT COD_DEPARTAMENTO, NO_DEPARTAMENTO FROM BI.DBO.[BI_CAD_PRODUTO]
	";
	r\[LeftArrow]mrtSQLDataObject[conn,sql];
	Rule@@@r["Data"]
]
maGetDeParaDpto[];


$lojas={1,2,3,6,7,9,12,13,17,18,20,21,22,23,24,25,27,29,30,31};
$dpto={1,2,3,6,9,10,12,13,21};
$deParaLoja=maGetDeParaLojas[];
$deParaDpto=maGetDeParaDpto[];
$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};


getESTOQUE[Dpto_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 
		
SET NOCOUNT ON;

-- =============================================
-- AUTOR		      : DIEGO MILLER
-- DATA DE CRIA\[CapitalCCedilla]\[CapitalATilde]O    : 09/03/2016
-- DESCRI\[CapitalCCedilla]\[CapitalATilde]O          : ESTOQUE CD VS ORBIS
-- BANCOS UTILIZADOS  : DW E ZEUS_RTG
-- DATAS DE ALTERACAO : --/--/----
-- =============================================

---------------------------------------------------------------------------
--ULTIMOS INVENTARIOS--
---------------------------------------------------------------------------

SELECT 
		COD_LOJA
		,ISNULL(MAX(CONVERT(DATE,DTA_INVENTARIO)),'2014-01-01') DTA_INV 
INTO #TMP_INV 
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_INVENTARIO 
GROUP BY COD_LOJA

---------------------------------------------------------------------------
--RELACIOANDO NOTAS DA ORBIS POS OS INVENTARIOS DE CADA LOJA--
---------------------------------------------------------------------------

SELECT 
		DET.COD_LOJA
		,CONVERT(DATE,XMLS.DTA_GRAVACAO) DTA_GRAVACAO
		,CONVERT(DATE,DTA_INV) DTA_INV
		,CAST(CONVERT(DATETIME,XMLS.DTA_GRAVACAO)-CONVERT(DATETIME,DTA_INV) AS DOUBLE PRECISION) VALIDACAO
		,CONVERT(DATE,DTA_PROCESSADO) DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,DET.COD_FORNECEDOR
		,CPROD
		,COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE 
		,CAST(QCOM AS DOUBLE PRECISION) QCOM
		,CAST(QEMBALAGEM AS DOUBLE PRECISION) QEMBALAGEM
		,CAST(QCOM*QEMBALAGEM AS DOUBLE PRECISION) QTOTAL
		,CAST(QTRIB AS DOUBLE PRECISION) QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,DET.NUM_DANFE
		,SUBSTRING(DET.NUM_DANFE, 26, 9)*1 NUM_NF
		,LEFT (SUBSTRING(DET.NUM_DANFE, 26, 9)*1,6) AS NF_NUMERO_DIREITA 
	    ,RIGHT (SUBSTRING(DET.NUM_DANFE, 26, 9)*1,6) AS NF_NUMERO_ESQUEDA 
INTO #XMLS	--DROP TABLE #XMLS
FROM [CTRLNFE].[DBO].NFE_DET DET 
		LEFT JOIN [CTRLNFE].[DBO].[NFE_ARQUIVOS_XMLS] XMLS 
				ON XMLS.CHAVE = DET.NUM_DANFE 
		LEFT JOIN CTRLNFE.DBO.NFE_STATUS STAT 
				ON STAT.NUM_DANFE = DET.NUM_DANFE
		LEFT JOIN #TMP_INV INV 
				ON INV.COD_LOJA = DET.COD_LOJA 
WHERE 1=1
		AND DET.COD_FORNECEDOR IN (18055,103256)
		AND CAST(CONVERT(DATETIME,XMLS.DTA_GRAVACAO)-CONVERT(DATETIME,DTA_INV) AS DOUBLE PRECISION) >= 0

---------------------------------------------------------------------------
--RELACIOANDO ENTRADAS NO ZEUS--
---------------------------------------------------------------------------

SELECT 
		COD_LOJA
		,DES_PRODUTO
		,COD_PRODUTO
		,QTD_ENTRADA
		,QTD_EMBALAGEM
		,QTD_ENTRADA*QTD_EMBALAGEM QTD_TOTAL
		,VAL_CUSTO_REP
		,COD_FISCAL
		,NUM_NF_FORN
		,COD_FORNECEDOR
		,DES_FORNECEDOR
		,DTA_ENTRADA
		,DTA_EMISSAO
		,DTA_GRAVACAO
		,USUARIO
		,NUM_DANFE
INTO #TMP_ENTRADAS_ZEUS --DROP TABLE #TMP_ENTRADAS_ZEUS
FROM [192.168.0.6].[ZEUS_RTG].[DBO].[VW_MARCHE_ENTRADAS] 
WHERE 1=1 
		AND COD_FORNECEDOR IN (18055,103256)
		AND CONVERT(DATE,DTA_ENTRADA) > '20150101'

---------------------------------------------------------------------------
--TIRANDO NOTAS JA LANCADAS ATRAVES DA CHAVE DE ACESSO--
---------------------------------------------------------------------------

SELECT 
		XMLS.COD_LOJA
		,XMLS.DTA_GRAVACAO
		,DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,XMLS.COD_FORNECEDOR
		,CPROD
		,XMLS.COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE
		,QCOM
		,QEMBALAGEM
		,QTOTAL
		,QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,XMLS.NUM_DANFE
		,NUM_NF
		,NF_NUMERO_DIREITA
		,NF_NUMERO_ESQUEDA
INTO #TEMP_PRIMEIRA --DROP TABLE #TEMP_PRIMEIRA
FROM  #XMLS  XMLS 
		LEFT JOIN  #TMP_ENTRADAS_ZEUS ZEUS 
				ON XMLS.CHAVE = ZEUS.NUM_DANFE COLLATE  SQL_LATIN1_GENERAL_CP1_CI_AS
WHERE 1=1
		AND ZEUS.NUM_DANFE IS  NULL

DROP TABLE #XMLS
DROP TABLE #TMP_INV

---------------------------------------------------------------------------
--TIRANDO NOTAS JA LANCADAS ATRAVES DO FORNECEDOR E NUMERO DA NOTA--
---------------------------------------------------------------------------

SELECT 
		XMLS.COD_LOJA
		,XMLS.DTA_GRAVACAO
		,DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,XMLS.COD_FORNECEDOR
		,CPROD
		,XMLS.COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE
		,QCOM
		,QEMBALAGEM
		,QTOTAL
		,QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,XMLS.NUM_DANFE
		,NUM_NF
		,NF_NUMERO_DIREITA
		,NF_NUMERO_ESQUEDA
INTO  #SEG_LIMPEZA --DROP TABLE #SEG_LIMPEZA
FROM  #TEMP_PRIMEIRA  XMLS LEFT JOIN  #TMP_ENTRADAS_ZEUS ZEUS ON ZEUS.COD_LOJA = XMLS.COD_LOJA AND ZEUS.COD_FORNECEDOR = XMLS.COD_FORNECEDOR AND ZEUS.NUM_NF_FORN = XMLS.NUM_NF
WHERE 1=1
		AND ZEUS.COD_LOJA IS  NULL

DROP TABLE #TEMP_PRIMEIRA
---------------------------------------------------------------------------
--TIRANDO NOTAS JA LANCADAS ATRAVES DO FORNECEDOR E NUMERO DA NOTA TIRANDO O NUMERO DA DIREITA--
---------------------------------------------------------------------------

SELECT 
		XMLS.COD_LOJA
		,XMLS.DTA_GRAVACAO
		,DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,XMLS.COD_FORNECEDOR
		,CPROD
		,XMLS.COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE
		,QCOM
		,QEMBALAGEM
		,QTOTAL
		,QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,XMLS.NUM_DANFE
		,NUM_NF
		,NF_NUMERO_DIREITA
		,NF_NUMERO_ESQUEDA
INTO  #TERC_LIMPEZA --DROP TABLE #TERC_LIMPEZA
FROM  #SEG_LIMPEZA  XMLS 
		LEFT JOIN  #TMP_ENTRADAS_ZEUS ZEUS 
			ON ZEUS.COD_LOJA = XMLS.COD_LOJA AND ZEUS.COD_FORNECEDOR = XMLS.COD_FORNECEDOR AND ZEUS.NUM_NF_FORN = XMLS.NF_NUMERO_DIREITA
WHERE ZEUS.COD_LOJA IS  NULL

DROP TABLE #SEG_LIMPEZA
---------------------------------------------------------------------------
--TIRANDO NOTAS JA LANCADAS ATRAVES DO FORNECEDOR E NUMERO DA NOTA TIRANDO O NUMERO DA ESQUERDA--
---------------------------------------------------------------------------

SELECT 
		XMLS.COD_LOJA
		,XMLS.DTA_GRAVACAO
		,DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,XMLS.COD_FORNECEDOR
		,CPROD
		,XMLS.COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE
		,QCOM
		,QEMBALAGEM
		,QTOTAL
		,QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,XMLS.NUM_DANFE
		,NUM_NF
		,NF_NUMERO_DIREITA
		,NF_NUMERO_ESQUEDA
INTO #TMP_ENTRADAS_PENDENTES
FROM  #TERC_LIMPEZA  XMLS --DROP TABLE #TERC_LIMPEZA
		LEFT JOIN  #TMP_ENTRADAS_ZEUS ZEUS 
				ON ZEUS.COD_LOJA = XMLS.COD_LOJA AND ZEUS.COD_FORNECEDOR = XMLS.COD_FORNECEDOR AND ZEUS.NUM_NF_FORN = XMLS.NF_NUMERO_ESQUEDA
WHERE 1=1
	AND ZEUS.COD_LOJA IS NULL
	AND CFOP NOT IN ('1201') -- DEVOLUCAO DE VENDA INTERNA DO FORNECEDOR

DROP TABLE #TERC_LIMPEZA
DROP TABLE #TMP_ENTRADAS_ZEUS


SELECT 
COD_LOJA,
COD_PRODUTO,
SUM(QTOTAL) QTDE
INTO #QTDE_PENDENTES
 FROM #TMP_ENTRADAS_PENDENTES
 GROUP BY COD_LOJA,
COD_PRODUTO

DROP TABLE #TMP_ENTRADAS_PENDENTES
---------------------------------------------------------------------------
--ESTOQUE ORBIS--
---------------------------------------------------------------------------

SELECT 
		COD_LOJA
		,COD_PRODUTO
		,DATA
		,QTD_ESTOQUE
INTO #TEMP_ESTOQUE 
FROM DW.DBO.ESTOQUE 
WHERE 1=1 
		AND COD_LOJA = 5 
		AND DATA =  CONVERT(DATE,GETDATE()) 
		AND QTD_ESTOQUE > 0

---------------------------------------------------------------------------
--ESTOQUE ORBIS--
---------------------------------------------------------------------------


SELECT 
		 NO_LOJA Loja
		,A.COD_PRODUTO Plu
		,DESCRICAO Descri\[CCedilla]\[ATilde]o
		,NO_DEPARTAMENTO Departamento
		,LINHA.CLASSIF_PRODUTO_LOJA ABC
		,LINHA.FORA_LINHA FL
		,CAST(A.QTD_ESTOQUE AS DOUBLE PRECISION) [Estoque Loja]
		,CONVERT(DOUBLE PRECISION,isnull(AVG_QTD_U30D_PD,0)) [VM 30D]
		,CONVERT(DOUBLE PRECISION,isnull(CASE WHEN A.QTD_ESTOQUE < 0 THEN 0  ELSE (A.QTD_ESTOQUE/AVG_QTD_U30D_PD) END,999)) AS DPD
		--,CONVERT(INT,isnull(A.QTD_ESTOQUE/AVG_QTD_U30D_PD,999)) as DPD
		,isnull(qtde,0) Transito
		,CAST(B.QTD_ESTOQUE AS DOUBLE PRECISION) [Estoque Orbis]
INTO #TMP_RESUMO
FROM DW.DBO.ESTOQUE A 
		LEFT JOIN #TEMP_ESTOQUE B 
				ON B.COD_PRODUTO = A.COD_PRODUTO 
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO CAD 
				ON CAD.COD_PRODUTO = A.COD_PRODUTO 
		LEFT JOIN BI.DBO.BI_LINHA_PRODUTOS LINHA
				ON LINHA.COD_LOJA = A.COD_LOJA AND LINHA.COD_PRODUTO = A.COD_PRODUTO 
		LEFT JOIN #QTDE_PENDENTES TRANSITO
				ON TRANSITO.cod_loja = A.COD_LOJA and TRANSITO.Cod_produto = A.Cod_produto
		left join bi.dbo.COMPRAS_ESTATISTICA_PRODUTO avg
		on avg.COD_LOJA= a.COD_LOJA and avg.COD_PRODUTO = a.COD_PRODUTO
		LEFT JOIN [BI].[DBO].[BI_CAD_LOJA2] LJ 
				ON LJ.COD_LOJA = A.COD_LOJA 
WHERE 1=1 
		AND A.COD_LOJA NOT IN (5,28,33)
		AND LINHA.FORA_LINHA = 'N'
		AND B.COD_PRODUTO IS NOT NULL
		AND A.DATA = CONVERT(DATE,GETDATE())
		AND CAD.COD_DEPARTAMENTO IN (`1`)

ORDER BY 
		 A.QTD_ESTOQUE ASC
		,B.QTD_ESTOQUE DESC
		

DROP TABLE #TEMP_ESTOQUE
DROP TABLE #QTDE_PENDENTES


SELECT TOP 10
			Loja
			,Plu
			,[Descri\[CCedilla]\[ATilde]o]
			,Departamento
			,ABC
			,FL
			,[Estoque Loja]
			,[VM 30D]
			,DPD
			,Transito
			,[Estoque Orbis]
FROM #TMP_RESUMO

DROP TABLE #TMP_RESUMO
";
r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@Dpto}];
	r["Dpto"]=Dpto;
	r["NO_DEPARTAMENTO"]=Dpto/.$deParaDpto;
	r["FileName"]=ToString@Row@{"Estoque Loja x Estoque CD ",r["NO_DEPARTAMENTO"]," ",$now2,".png"};
	r
]
r[80]=getESTOQUE[$dpto];


i=0;Dynamic@i
Scan[(i++;r[#]=getESTOQUE[#])&,$dpto]


gridESTOQUE[data_Symbol]:=Module[{grid,title,r,color,color2,stl,stl2,stl3,Dpto},
	
r\[LeftArrow]data;
			color2=Which[
			#<0.04,Red
			,#<0.06,Orange
			,True,Lighter@Blue
	]&;

	stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="";


			color=Which[
			#<0.04,Red
			,#<0.06,Red
			,True,Red
	]&;

	stl2[val_]:=Style[maF["N"][val],Bold,color@val];
	stl2[Null]="";

	stl3[val_]:=Style[maF["N,00"][val],Bold];
	stl3[Null]="";


	grid=maReportGrid[r,"ColumnFormat"->{{"Estoque Loja","Transito"}-> stl,{"Estoque Orbis"}->stl2,{"VM 30D","DPD"}->stl3},"TotalRow"->False];
	title=Style[Row@{"Estoque Loja x Estoque CD em ", $now},maTitleFormatOptions];
	(maReportFrame[title,grid])
]
gridESTOQUE[r[80]]


$DptoAll=Export[ToString@Row@{"Estoque Loja x Estoque CD Rede ",$now2,".png"},gridESTOQUE[r[80]]];


createFile[$dpto_Integer]:=Module[{},
	Export[r[$dpto]["FileName"],gridESTOQUE[r[$dpto]]]
]
i=0;Dynamic@i
Scan[(i++;createFile[#])&,$dpto[[]]]


getESTOQUEEXCEL[Dpto_]:=Module[{sql,e,conn=marcheConn2[],tab},
	sql=" 
		
SET NOCOUNT ON;
-- =============================================
-- AUTOR		      : DIEGO MILLER
-- DATA DE CRIA\[CapitalCCedilla]\[CapitalATilde]O    : 09/03/2016
-- DESCRI\[CapitalCCedilla]\[CapitalATilde]O          : ESTOQUE CD VS ORBIS
-- BANCOS UTILIZADOS  : DW E ZEUS_RTG
-- DATAS DE ALTERACAO : --/--/----
-- =============================================

---------------------------------------------------------------------------
--ULTIMOS INVENTARIOS--
---------------------------------------------------------------------------

SELECT 
		COD_LOJA
		,ISNULL(MAX(CONVERT(DATE,DTA_INVENTARIO)),'2014-01-01') DTA_INV 
INTO #TMP_INV 
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_INVENTARIO 
GROUP BY COD_LOJA

---------------------------------------------------------------------------
--RELACIOANDO NOTAS DA ORBIS POS OS INVENTARIOS DE CADA LOJA--
---------------------------------------------------------------------------

SELECT 
		DET.COD_LOJA
		,CONVERT(DATE,XMLS.DTA_GRAVACAO) DTA_GRAVACAO
		,CONVERT(DATE,DTA_INV) DTA_INV
		,CAST(CONVERT(DATETIME,XMLS.DTA_GRAVACAO)-CONVERT(DATETIME,DTA_INV) AS DOUBLE PRECISION) VALIDACAO
		,CONVERT(DATE,DTA_PROCESSADO) DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,DET.COD_FORNECEDOR
		,CPROD
		,COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE 
		,CAST(QCOM AS DOUBLE PRECISION) QCOM
		,CAST(QEMBALAGEM AS DOUBLE PRECISION) QEMBALAGEM
		,CAST(QCOM*QEMBALAGEM AS DOUBLE PRECISION) QTOTAL
		,CAST(QTRIB AS DOUBLE PRECISION) QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,DET.NUM_DANFE
		,SUBSTRING(DET.NUM_DANFE, 26, 9)*1 NUM_NF
		,LEFT (SUBSTRING(DET.NUM_DANFE, 26, 9)*1,6) AS NF_NUMERO_DIREITA 
	    ,RIGHT (SUBSTRING(DET.NUM_DANFE, 26, 9)*1,6) AS NF_NUMERO_ESQUEDA 
INTO #XMLS	--DROP TABLE #XMLS
FROM [CTRLNFE].[DBO].NFE_DET DET 
		LEFT JOIN [CTRLNFE].[DBO].[NFE_ARQUIVOS_XMLS] XMLS 
				ON XMLS.CHAVE = DET.NUM_DANFE 
		LEFT JOIN CTRLNFE.DBO.NFE_STATUS STAT 
				ON STAT.NUM_DANFE = DET.NUM_DANFE
		LEFT JOIN #TMP_INV INV 
				ON INV.COD_LOJA = DET.COD_LOJA 
WHERE 1=1
		AND DET.COD_FORNECEDOR IN (18055,103256)
		AND CAST(CONVERT(DATETIME,XMLS.DTA_GRAVACAO)-CONVERT(DATETIME,DTA_INV) AS DOUBLE PRECISION) >= 0

---------------------------------------------------------------------------
--RELACIOANDO ENTRADAS NO ZEUS--
---------------------------------------------------------------------------

SELECT 
		COD_LOJA
		,DES_PRODUTO
		,COD_PRODUTO
		,QTD_ENTRADA
		,QTD_EMBALAGEM
		,QTD_ENTRADA*QTD_EMBALAGEM QTD_TOTAL
		,VAL_CUSTO_REP
		,COD_FISCAL
		,NUM_NF_FORN
		,COD_FORNECEDOR
		,DES_FORNECEDOR
		,DTA_ENTRADA
		,DTA_EMISSAO
		,DTA_GRAVACAO
		,USUARIO
		,NUM_DANFE
INTO #TMP_ENTRADAS_ZEUS --DROP TABLE #TMP_ENTRADAS_ZEUS
FROM [192.168.0.6].[ZEUS_RTG].[DBO].[VW_MARCHE_ENTRADAS] 
WHERE 1=1 
		AND COD_FORNECEDOR IN (18055,103256)
		AND CONVERT(DATE,DTA_ENTRADA) > '20150101'

---------------------------------------------------------------------------
--TIRANDO NOTAS JA LANCADAS ATRAVES DA CHAVE DE ACESSO--
---------------------------------------------------------------------------

SELECT 
		XMLS.COD_LOJA
		,XMLS.DTA_GRAVACAO
		,DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,XMLS.COD_FORNECEDOR
		,CPROD
		,XMLS.COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE
		,QCOM
		,QEMBALAGEM
		,QTOTAL
		,QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,XMLS.NUM_DANFE
		,NUM_NF
		,NF_NUMERO_DIREITA
		,NF_NUMERO_ESQUEDA
INTO #TEMP_PRIMEIRA --DROP TABLE #TEMP_PRIMEIRA
FROM  #XMLS  XMLS 
		LEFT JOIN  #TMP_ENTRADAS_ZEUS ZEUS 
				ON XMLS.CHAVE = ZEUS.NUM_DANFE COLLATE  SQL_LATIN1_GENERAL_CP1_CI_AS
WHERE 1=1
		AND ZEUS.NUM_DANFE IS  NULL

DROP TABLE #XMLS
DROP TABLE #TMP_INV

---------------------------------------------------------------------------
--TIRANDO NOTAS JA LANCADAS ATRAVES DO FORNECEDOR E NUMERO DA NOTA--
---------------------------------------------------------------------------

SELECT 
		XMLS.COD_LOJA
		,XMLS.DTA_GRAVACAO
		,DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,XMLS.COD_FORNECEDOR
		,CPROD
		,XMLS.COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE
		,QCOM
		,QEMBALAGEM
		,QTOTAL
		,QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,XMLS.NUM_DANFE
		,NUM_NF
		,NF_NUMERO_DIREITA
		,NF_NUMERO_ESQUEDA
INTO  #SEG_LIMPEZA --DROP TABLE #SEG_LIMPEZA
FROM  #TEMP_PRIMEIRA  XMLS LEFT JOIN  #TMP_ENTRADAS_ZEUS ZEUS ON ZEUS.COD_LOJA = XMLS.COD_LOJA AND ZEUS.COD_FORNECEDOR = XMLS.COD_FORNECEDOR AND ZEUS.NUM_NF_FORN = XMLS.NUM_NF
WHERE 1=1
		AND ZEUS.COD_LOJA IS  NULL

DROP TABLE #TEMP_PRIMEIRA
---------------------------------------------------------------------------
--TIRANDO NOTAS JA LANCADAS ATRAVES DO FORNECEDOR E NUMERO DA NOTA TIRANDO O NUMERO DA DIREITA--
---------------------------------------------------------------------------

SELECT 
		XMLS.COD_LOJA
		,XMLS.DTA_GRAVACAO
		,DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,XMLS.COD_FORNECEDOR
		,CPROD
		,XMLS.COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE
		,QCOM
		,QEMBALAGEM
		,QTOTAL
		,QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,XMLS.NUM_DANFE
		,NUM_NF
		,NF_NUMERO_DIREITA
		,NF_NUMERO_ESQUEDA
INTO  #TERC_LIMPEZA --DROP TABLE #TERC_LIMPEZA
FROM  #SEG_LIMPEZA  XMLS 
		LEFT JOIN  #TMP_ENTRADAS_ZEUS ZEUS 
			ON ZEUS.COD_LOJA = XMLS.COD_LOJA AND ZEUS.COD_FORNECEDOR = XMLS.COD_FORNECEDOR AND ZEUS.NUM_NF_FORN = XMLS.NF_NUMERO_DIREITA
WHERE ZEUS.COD_LOJA IS  NULL

DROP TABLE #SEG_LIMPEZA
---------------------------------------------------------------------------
--TIRANDO NOTAS JA LANCADAS ATRAVES DO FORNECEDOR E NUMERO DA NOTA TIRANDO O NUMERO DA ESQUERDA--
---------------------------------------------------------------------------

SELECT 
		XMLS.COD_LOJA
		,XMLS.DTA_GRAVACAO
		,DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,XMLS.COD_FORNECEDOR
		,CPROD
		,XMLS.COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE
		,QCOM
		,QEMBALAGEM
		,QTOTAL
		,QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,XMLS.NUM_DANFE
		,NUM_NF
		,NF_NUMERO_DIREITA
		,NF_NUMERO_ESQUEDA
INTO #TMP_ENTRADAS_PENDENTES
FROM  #TERC_LIMPEZA  XMLS --DROP TABLE #TERC_LIMPEZA
		LEFT JOIN  #TMP_ENTRADAS_ZEUS ZEUS 
				ON ZEUS.COD_LOJA = XMLS.COD_LOJA AND ZEUS.COD_FORNECEDOR = XMLS.COD_FORNECEDOR AND ZEUS.NUM_NF_FORN = XMLS.NF_NUMERO_ESQUEDA
WHERE 1=1
	AND ZEUS.COD_LOJA IS NULL
	AND CFOP NOT IN ('1201') -- DEVOLUCAO DE VENDA INTERNA DO FORNECEDOR

DROP TABLE #TERC_LIMPEZA
DROP TABLE #TMP_ENTRADAS_ZEUS


SELECT 
COD_LOJA,
COD_PRODUTO,
SUM(QTOTAL) QTDE
INTO #QTDE_PENDENTES
 FROM #TMP_ENTRADAS_PENDENTES
 GROUP BY COD_LOJA,
COD_PRODUTO

DROP TABLE #TMP_ENTRADAS_PENDENTES
---------------------------------------------------------------------------
--ESTOQUE ORBIS--
---------------------------------------------------------------------------

SELECT 
		COD_LOJA
		,COD_PRODUTO
		,DATA
		,QTD_ESTOQUE
INTO #TEMP_ESTOQUE 
FROM DW.DBO.ESTOQUE 
WHERE 1=1 
		AND COD_LOJA = 5 
		AND DATA =  CONVERT(DATE,GETDATE()) 
		AND QTD_ESTOQUE > 0

---------------------------------------------------------------------------
--ESTOQUE ORBIS--
---------------------------------------------------------------------------


SELECT 
		 NO_LOJA Loja
		,A.COD_PRODUTO Plu
		,DESCRICAO Descri\[CCedilla]\[ATilde]o
		,NO_DEPARTAMENTO Departamento
		,LINHA.CLASSIF_PRODUTO_LOJA ABC
		,LINHA.FORA_LINHA FL
		,CAST(A.QTD_ESTOQUE AS DOUBLE PRECISION) [Estoque Loja]
		,CONVERT(DOUBLE PRECISION,isnull(AVG_QTD_U30D_PD,0)) [VM 30D]
		,CONVERT(DOUBLE PRECISION,isnull(CASE WHEN A.QTD_ESTOQUE < 0 THEN 0  ELSE (A.QTD_ESTOQUE/AVG_QTD_U30D_PD) END,999)) AS DPD
		--,CONVERT(INT,isnull(A.QTD_ESTOQUE/AVG_QTD_U30D_PD,999)) as DPD
		,isnull(qtde,0) Transito
		,CAST(B.QTD_ESTOQUE AS DOUBLE PRECISION) [Estoque Orbis]
FROM DW.DBO.ESTOQUE A 
		LEFT JOIN #TEMP_ESTOQUE B 
				ON B.COD_PRODUTO = A.COD_PRODUTO 
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO CAD 
				ON CAD.COD_PRODUTO = A.COD_PRODUTO 
		LEFT JOIN BI.DBO.BI_LINHA_PRODUTOS LINHA
				ON LINHA.COD_LOJA = A.COD_LOJA AND LINHA.COD_PRODUTO = A.COD_PRODUTO 
		LEFT JOIN #QTDE_PENDENTES TRANSITO
				ON TRANSITO.cod_loja = A.COD_LOJA and TRANSITO.Cod_produto = A.Cod_produto
		left join bi.dbo.COMPRAS_ESTATISTICA_PRODUTO avg
		on avg.COD_LOJA= a.COD_LOJA and avg.COD_PRODUTO = a.COD_PRODUTO
		LEFT JOIN [BI].[DBO].[BI_CAD_LOJA2] LJ 
				ON LJ.COD_LOJA = A.COD_LOJA 
WHERE 1=1 
		AND A.COD_LOJA NOT IN (5,28,33)
		AND LINHA.FORA_LINHA = 'N'
		AND B.COD_PRODUTO IS NOT NULL
		AND A.DATA = CONVERT(DATE,GETDATE())
		--AND A.QTD_ESTOQUE  >= 0  
		--AND  A.QTD_ESTOQUE <= 12
		AND CAD.COD_DEPARTAMENTO IN (`1`)

ORDER BY 
		 A.QTD_ESTOQUE ASC
		,B.QTD_ESTOQUE DESC

DROP TABLE #TEMP_ESTOQUE
DROP TABLE #QTDE_PENDENTES


";
	e\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@Dpto}];
	e["Dpto"]=Dpto;
	e["NO_DEPARTAMENTO"]=Dpto/.$deParaDpto;
	e["FileName"]=ToString@Row@{"Estoque Loja x Estoque CD ",e["NO_DEPARTAMENTO"]," ",$now2,".png"};
	e
]
e[10]=getESTOQUEEXCEL[$dpto];


$fileNameEXECEL="Estoque CD Vs Loja "<>$now2<>".xlsx";
Export["Estoque CD Vs Loja "<>$now2<>".xlsx",e[10]["DataAll"]];


getNFS[Dpto_]:=Module[{sql,e,conn=marcheConn2[],tab},
	sql=" 
		
-- =============================================
-- AUTOR		      : DIEGO MILLER
-- DATA DE CRIA\[CapitalCCedilla]\[CapitalATilde]O    : 09/03/2016
-- DESCRI\[CapitalCCedilla]\[CapitalATilde]O          : ESTOQUE CD VS ORBIS
-- BANCOS UTILIZADOS  : DW E ZEUS_RTG
-- DATAS DE ALTERACAO : --/--/----
-- =============================================

SET NOCOUNT ON;
---------------------------------------------------------------------------
--ULTIMOS INVENTARIOS--
---------------------------------------------------------------------------

SELECT 
		COD_LOJA
		,ISNULL(MAX(CONVERT(DATE,DTA_INVENTARIO)),'2014-01-01') DTA_INV 
INTO #TMP_INV 
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_INVENTARIO 
GROUP BY COD_LOJA

---------------------------------------------------------------------------
--RELACIOANDO NOTAS DA ORBIS POS OS INVENTARIOS DE CADA LOJA--
---------------------------------------------------------------------------

SELECT 
		DET.COD_LOJA
		,CONVERT(DATE,XMLS.DTA_GRAVACAO) DTA_GRAVACAO
		,CONVERT(DATE,DTA_INV) DTA_INV
		,CAST(CONVERT(DATETIME,XMLS.DTA_GRAVACAO)-CONVERT(DATETIME,DTA_INV) AS DOUBLE PRECISION) VALIDACAO
		,CONVERT(DATE,DTA_PROCESSADO) DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,DET.COD_FORNECEDOR
		,CPROD
		,COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE 
		,CAST(QCOM AS DOUBLE PRECISION) QCOM
		,CAST(QEMBALAGEM AS DOUBLE PRECISION) QEMBALAGEM
		,CAST(QCOM*QEMBALAGEM AS DOUBLE PRECISION) QTOTAL
		,CAST(QTRIB AS DOUBLE PRECISION) QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,DET.NUM_DANFE
		,SUBSTRING(DET.NUM_DANFE, 26, 9)*1 NUM_NF
		,LEFT (SUBSTRING(DET.NUM_DANFE, 26, 9)*1,6) AS NF_NUMERO_DIREITA 
	    ,RIGHT (SUBSTRING(DET.NUM_DANFE, 26, 9)*1,6) AS NF_NUMERO_ESQUEDA 
INTO #XMLS	--DROP TABLE #XMLS
FROM [CTRLNFE].[DBO].NFE_DET DET 
		LEFT JOIN [CTRLNFE].[DBO].[NFE_ARQUIVOS_XMLS] XMLS 
				ON XMLS.CHAVE = DET.NUM_DANFE 
		LEFT JOIN CTRLNFE.DBO.NFE_STATUS STAT 
				ON STAT.NUM_DANFE = DET.NUM_DANFE
		LEFT JOIN #TMP_INV INV 
				ON INV.COD_LOJA = DET.COD_LOJA 
WHERE 1=1
		AND DET.COD_FORNECEDOR IN (18055,103256)
		AND CAST(CONVERT(DATETIME,XMLS.DTA_GRAVACAO)-CONVERT(DATETIME,DTA_INV) AS DOUBLE PRECISION) >= 0

---------------------------------------------------------------------------
--RELACIOANDO ENTRADAS NO ZEUS--
---------------------------------------------------------------------------

SELECT 
		COD_LOJA
		,DES_PRODUTO
		,COD_PRODUTO
		,QTD_ENTRADA
		,QTD_EMBALAGEM
		,QTD_ENTRADA*QTD_EMBALAGEM QTD_TOTAL
		,VAL_CUSTO_REP
		,COD_FISCAL
		,NUM_NF_FORN
		,COD_FORNECEDOR
		,DES_FORNECEDOR
		,DTA_ENTRADA
		,DTA_EMISSAO
		,DTA_GRAVACAO
		,USUARIO
		,NUM_DANFE
INTO #TMP_ENTRADAS_ZEUS --DROP TABLE #TMP_ENTRADAS_ZEUS
FROM [192.168.0.6].[ZEUS_RTG].[DBO].[VW_MARCHE_ENTRADAS] 
WHERE 1=1 
		AND COD_FORNECEDOR IN (18055,103256)
		AND CONVERT(DATE,DTA_ENTRADA) > '20150101'

---------------------------------------------------------------------------
--TIRANDO NOTAS JA LANCADAS ATRAVES DA CHAVE DE ACESSO--
---------------------------------------------------------------------------

SELECT 
		XMLS.COD_LOJA
		,XMLS.DTA_GRAVACAO
		,DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,XMLS.COD_FORNECEDOR
		,CPROD
		,XMLS.COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE
		,QCOM
		,QEMBALAGEM
		,QTOTAL
		,QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,XMLS.NUM_DANFE
		,NUM_NF
		,NF_NUMERO_DIREITA
		,NF_NUMERO_ESQUEDA
INTO #TEMP_PRIMEIRA --DROP TABLE #TEMP_PRIMEIRA
FROM  #XMLS  XMLS 
		LEFT JOIN  #TMP_ENTRADAS_ZEUS ZEUS 
				ON XMLS.CHAVE = ZEUS.NUM_DANFE COLLATE  SQL_LATIN1_GENERAL_CP1_CI_AS
WHERE 1=1
		AND ZEUS.NUM_DANFE IS  NULL

DROP TABLE #XMLS
DROP TABLE #TMP_INV

---------------------------------------------------------------------------
--TIRANDO NOTAS JA LANCADAS ATRAVES DO FORNECEDOR E NUMERO DA NOTA--
---------------------------------------------------------------------------

SELECT 
		XMLS.COD_LOJA
		,XMLS.DTA_GRAVACAO
		,DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,XMLS.COD_FORNECEDOR
		,CPROD
		,XMLS.COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE
		,QCOM
		,QEMBALAGEM
		,QTOTAL
		,QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,XMLS.NUM_DANFE
		,NUM_NF
		,NF_NUMERO_DIREITA
		,NF_NUMERO_ESQUEDA
INTO  #SEG_LIMPEZA --DROP TABLE #SEG_LIMPEZA
FROM  #TEMP_PRIMEIRA  XMLS LEFT JOIN  #TMP_ENTRADAS_ZEUS ZEUS ON ZEUS.COD_LOJA = XMLS.COD_LOJA AND ZEUS.COD_FORNECEDOR = XMLS.COD_FORNECEDOR AND ZEUS.NUM_NF_FORN = XMLS.NUM_NF
WHERE 1=1
		AND ZEUS.COD_LOJA IS  NULL

DROP TABLE #TEMP_PRIMEIRA
---------------------------------------------------------------------------
--TIRANDO NOTAS JA LANCADAS ATRAVES DO FORNECEDOR E NUMERO DA NOTA TIRANDO O NUMERO DA DIREITA--
---------------------------------------------------------------------------

SELECT 
		XMLS.COD_LOJA
		,XMLS.DTA_GRAVACAO
		,DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,XMLS.COD_FORNECEDOR
		,CPROD
		,XMLS.COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE
		,QCOM
		,QEMBALAGEM
		,QTOTAL
		,QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,XMLS.NUM_DANFE
		,NUM_NF
		,NF_NUMERO_DIREITA
		,NF_NUMERO_ESQUEDA
INTO  #TERC_LIMPEZA --DROP TABLE #TERC_LIMPEZA
FROM  #SEG_LIMPEZA  XMLS 
		LEFT JOIN  #TMP_ENTRADAS_ZEUS ZEUS 
			ON ZEUS.COD_LOJA = XMLS.COD_LOJA AND ZEUS.COD_FORNECEDOR = XMLS.COD_FORNECEDOR AND ZEUS.NUM_NF_FORN = XMLS.NF_NUMERO_DIREITA
WHERE ZEUS.COD_LOJA IS  NULL

DROP TABLE #SEG_LIMPEZA
---------------------------------------------------------------------------
--TIRANDO NOTAS JA LANCADAS ATRAVES DO FORNECEDOR E NUMERO DA NOTA TIRANDO O NUMERO DA ESQUERDA--
---------------------------------------------------------------------------

SELECT 
		XMLS.COD_LOJA
		,XMLS.DTA_GRAVACAO
		,DTA_PROCESSADO
		,FLG_RECEBIMENTO
		,XMLS.COD_FORNECEDOR
		,CPROD
		,XMLS.COD_PRODUTO
		,CEAN
		,XPROD
		,UCOM
		,CUNIDADE
		,QCOM
		,QEMBALAGEM
		,QTOTAL
		,QTRIB
		,VUNTRIB
		,VUNCOM
		,VPROD
		,CFOP
		,CHAVE
		,XMLS.NUM_DANFE
		,NUM_NF
		,NF_NUMERO_DIREITA
		,NF_NUMERO_ESQUEDA
FROM  #TERC_LIMPEZA  XMLS --DROP TABLE #TERC_LIMPEZA
		LEFT JOIN  #TMP_ENTRADAS_ZEUS ZEUS 
				ON ZEUS.COD_LOJA = XMLS.COD_LOJA AND ZEUS.COD_FORNECEDOR = XMLS.COD_FORNECEDOR AND ZEUS.NUM_NF_FORN = XMLS.NF_NUMERO_ESQUEDA
WHERE 1=1
	AND ZEUS.COD_LOJA IS NULL
	AND CFOP NOT IN ('1201') -- DEVOLUCAO DE VENDA INTERNA DO FORNECEDOR
	AND Xmls.cod_loja NOT IN (5,28,33)
	

Select top 1 * into #tmp_teste from BI.DBO.BI_CAD_PRODUTO where 1=1 and cod_departamento in (`1`)

";
	e\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@Dpto}];
	e["Dpto"]=Dpto;
	e["NO_DEPARTAMENTO"]=Dpto/.$deParaDpto;
	e["FileName"]=ToString@Row@{"Estoque Loja x Estoque CD ",e["NO_DEPARTAMENTO"]," ",$now2,".png"};
	e
]
e[12]=getNFS[$dpto];


$fileNameNFS="Rela\[CCedilla]\[ATilde]o de notas em transito "<>$now2<>".xlsx";
Export["Rela\[CCedilla]\[ATilde]o de notas em transito "<>$now2<>".xlsx",e[12]["DataAll"]];




marcheMail[
           "[GE] Estoque Loja x Estoque CD em " <>$now
  		,"Estoque Loja x Estoque CD

Filtros utilizados:

- Apenas produtos que estiverem em linha na loja
- Departamentos desconsiderados no relat\[OAcute]rio:

	4 - FLV
	5 - ACOUGUE
	7 - OUTROS
	8 - ROTISSERIE
	11 - CONGELADO
	14 - SAZONAL
	15 - INSUMOS
	16 - RESTAURANTE
	17 - IMPORTACAO
	18 - NAO DEFINIDO
	19 - BAZAR
	20 - NAO REVENDA
	99 - FINANCEIRO
"
  		,$mailsGerencia
  		,{$DptoAll,r[1]["FileName"],r[2]["FileName"],r[3]["FileName"],r[6]["FileName"],r[9]["FileName"],r[10]["FileName"],r[12]["FileName"],r[13]["FileName"],r[21]["FileName"],$fileNameEXECEL,$fileNameNFS}
]



(*
marcheMail[
           "Estoque Loja x Estoque CD em " <>$now
  		,"Valida\[CCedilla]\[ATilde]o Relat\[OAcute]rio - Estoque Loja x Estoque CD"
  		,$mailsGerencia
  		,{$DptoAll,r[1]["FileName"],r[2]["FileName"],r[3]["FileName"],r[4]["FileName"],r[5]["FileName"],r[6]["FileName"],r[7]["FileName"],r[8]["FileName"],r[9]["FileName"],r[10]["FileName"],r[11]["FileName"],r[12]["FileName"],r[13]["FileName"],r[14]["FileName"],r[15]["FileName"],r[19]["FileName"],r[21]["FileName"],$fileNameEXECEL,$fileNameNFS}
]
*)


(*
marcheMail[
           "Estoque Loja x Estoque CD em " <>$now
  		,"Estoque Loja x Estoque CD

Filtros utilizados:

- Apenas produtos que estiverem em linha na loja
- Departamentos desconsiderados no relat\[OAcute]rio:

	4 - FLV
	5 - ACOUGUE
	7 - OUTROS
	8 - ROTISSERIE
	11 - CONGELADO
	14 - SAZONAL
	15 - INSUMOS
	16 - RESTAURANTE
	17 - IMPORTACAO
	18 - NAO DEFINIDO
	19 - BAZAR
	20 - NAO REVENDA
	99 - FINANCEIRO


			"
  		,"diego.miller@marche.com.br"
  		,{$DptoAll,r[1]["FileName"],r[2]["FileName"],r[3]["FileName"],r[6]["FileName"],r[9]["FileName"],r[10]["FileName"],r[12]["FileName"],r[13]["FileName"],r[21]["FileName"],$fileNameEXECEL,$fileNameNFS}
]
*)
