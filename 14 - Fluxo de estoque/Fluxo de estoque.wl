(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["Marche.png"];
$lojas={1,2,3,5,6,7,9,12,13,17,18,20,21,22,23,24,25,27,29,30,31,33,9999};
$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[DatePlus[-0],{"Day","-","Month","-","Year"}];
$DateOntem=DateString[DatePlus[-1], {"Day", "/", "Month", "/", "Year"}];
$dataIni={2016,01,01};
$dataIniAnalise = DateString[$dataIni, {"Day", "/", "Month", "/", "Year"}];


DateString[$dataIni, {"Day", "/", "Month", "/", "Year"}];
SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};


(* ::Title:: *)
(*DADOS PARA GRID*)


getValidacaoInv[codLoja_,dtIni_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 
SET NOCOUNT ON;

SELECT 
		ID
		,CONVERT(DATE,DATA) AS DATA
		,ARQUIVO
		,COD_LOJA
		,COD_EAN
		,CAST(QTDE AS DOUBLE PRECISION) AS QTDE_ARQUIVO
		,CAST(QTDE_EST_DIA AS DOUBLE PRECISION) AS QTDE_EST_DIA
		,CAST(QTDE_AJUSTE AS DOUBLE PRECISION) AS QTDE_AJUSTE
		,CAST (QTDE_CONTAGEM AS DOUBLE PRECISION) AS QTDE_CONTAGEM 
		,CONVERT(DATE,DTA_INVENTARIO) AS DTA_INVENTARIO
		,COD_PRODUTO
		,STATUS
		,(CASE WHEN  QTDE_AJUSTE > 0 THEN '300'  ELSE '199' END) AS COD_AJUSTE
INTO #TEMP_TAB_IMPORT_ARQ
FROM 
		INTRANET.DBO.TAB_INVENTROTATIVO_CTRL AS TAB 
WHERE 1=1 
		AND DTA_INVENTARIO >= (`2`)	
		AND QTDE_AJUSTE <> 0
SELECT 
		TAB.COD_LOJA AS COD_LOJA
		,COUNT(STATUS) AS QTDE_ERROS
INTO #TEMP_TOTAL_PENDENCIAS_LOJAS
FROM 
		#TEMP_TAB_IMPORT_ARQ AS TAB 
LEFT JOIN TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK)
		ON AJUSTE.COD_LOJA = TAB.COD_LOJA AND AJUSTE.COD_AJUSTE = TAB.COD_AJUSTE AND AJUSTE.COD_PRODUTO = TAB.COD_PRODUTO AND AJUSTE.DTA_AJUSTE = TAB.DTA_INVENTARIO

WHERE 1=1 
		AND COD_AJUSTE_ESTOQUE IS NULL
GROUP BY 
		TAB.COD_LOJA

SELECT 
		A.COD_LOJA
		,CASE A.COD_LOJA WHEN 4 THEN 'Cotoxo' WHEN 10 THEN 'SCS' WHEN 19 THEN 'Sumare' WHEN 28 THEN 'Orbis' ELSE [NO_LOJA] END AS Loja
		,ISNULL(convert(varchar,QTDE_ERROS),0) as Qtde
INTO #TOTAL_TMP
FROM [192.168.0.13].[BI].[DBO].[BI_CAD_LOJA2] AS A LEFT JOIN #TEMP_TOTAL_PENDENCIAS_LOJAS AS PD ON A.COD_LOJA = PD.COD_LOJA
WHERE 1=1 
AND A.COD_LOJA NOT IN (4,10,19)
UNION
SELECT 9999 as COD_LOJA, 'Total' as 'Total', SUM(QTDE_ERROS) AS 'Total' from #TEMP_TOTAL_PENDENCIAS_LOJAS

ORDER BY 
	A.COD_LOJA

select Loja, Qtde from #TOTAL_TMP
WHERE 1=1		
		AND COD_LOJA IN (`1`)			
";
		r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLDateTime@dtIni}];
		r
];
r4\[LeftArrow]getValidacaoInv[$lojas,$dataIni]


getQuebras[codLoja_,dtIni_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 
SET NOCOUNT ON;



DECLARE @DTA_INI AS DATE
DECLARE @DTA_FIM AS DATE
DECLARE @CONSULTA BIT = 1

IF @DTA_INI IS NULL BEGIN SET @DTA_INI = '2016-01-01' END
IF @DTA_FIM IS NULL BEGIN SET @DTA_FIM = CAST(GETDATE()-2 AS DATE) END

IF OBJECT_ID('tempdb..#TMP_EAN') IS NOT NULL   

BEGIN
	DROP TABLE #TMP_EAN
END	


IF OBJECT_ID('tempdb..#TEMP_AJUSTE_INTRANET') IS NOT NULL   

BEGIN
	DROP TABLE #TEMP_AJUSTE_INTRANET
END	


IF OBJECT_ID('tempdb..#TEMP_AJUSTE_EFETIVADOS_ZEUS') IS NOT NULL   

BEGIN
	DROP TABLE #TEMP_AJUSTE_EFETIVADOS_ZEUS
END	


SELECT 
		CAST(COD_EAN AS DOUBLE PRECISION) AS COD_EAN, 
		CAST(COD_PRODUTO AS DOUBLE PRECISION) AS COD_PRODUTO 
INTO #TMP_EAN 
FROM ZEUS_RTG.DBO.TAB_CODIGO_BARRA (NOLOCK)

SELECT
		COD_LOJA
		,TIPO AS COD_AJUSTE
		,CAST(COD_PRODUTO AS DOUBLE PRECISION) AS COD_PRODUTO
		,SUM(CAST(QTDE AS DOUBLE PRECISION)) AS QTDE_AJUSTE
		,CAST(DATA AS DATE) DTA_AJUSTE
INTO #TEMP_AJUSTE_INTRANET
FROM 
		[INTRANET].[DBO].[TAB_TRANSFINTERNAS] AS T
INNER JOIN [INTRANET].[DBO].[TAB_TRANSFINTERNAS_PROD] AS P
		ON T.ID =P.IDTRANSF
LEFT JOIN #TMP_EAN AS ZEUS_BARRA (NOLOCK)
		ON CAST(P.COD_EAN AS DOUBLE PRECISION) = CAST(ZEUS_BARRA.COD_EAN AS DOUBLE PRECISION)
WHERE 1=1	
		AND CONVERT(DATE, DATA) >= CONVERT(DATE,@DTA_INI)
		AND CONVERT(DATE, DATA) <= CONVERT(DATE,@DTA_FIM)
		AND STATUS = 7	
GROUP BY
	 COD_LOJA,TIPO, CAST(COD_PRODUTO AS DOUBLE PRECISION),CAST(DATA AS DATE)

SELECT 
		COD_LOJA
		,COD_AJUSTE
		,CAST(COD_PRODUTO AS DOUBLE PRECISION) AS COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION) ) AS QTD_AJUSTE
		,CAST(DTA_AJUSTE AS DATE) DTA_AJUSTE
INTO #TEMP_AJUSTE_EFETIVADOS_ZEUS
FROM 
		ZEUS_RTG.DBO.TAB_AJUSTE_ESTOQUE WITH (NOLOCK)
WHERE 1=1	
		AND CONVERT(DATE, DTA_AJUSTE) >= CONVERT(DATE,@DTA_INI)
		AND CONVERT(DATE, DTA_AJUSTE) <= CONVERT(DATE,@DTA_FIM)
GROUP BY
	 COD_LOJA,COD_AJUSTE, CAST(COD_PRODUTO AS DOUBLE PRECISION),CAST(DTA_AJUSTE AS DATE)

IF @CONSULTA = 1

--		BEGIN

--				SELECT 
--				INTRANET.*
--				,ZEUS.*
--				,CASE WHEN TIPO_OPERACAO = 1 THEN  QTDE_AJUSTE *-1 
--						WHEN TIPO_OPERACAO = 2 THEN QTDE_AJUSTE *-1
--				ELSE QTDE_AJUSTE END - QTD_AJUSTE AS DIF
--				INTO #TEMP_TOTAL_SEM_CUSTO
--				FROM #TEMP_AJUSTE_INTRANET AS INTRANET 
--				LEFT JOIN ZEUS_RTG.DBO.TAB_TIPO_AJUSTE TIPO
--						ON TIPO.COD_AJUSTE = INTRANET.COD_AJUSTE
--				LEFT JOIN #TEMP_AJUSTE_EFETIVADOS_ZEUS AS ZEUS 
--						ON INTRANET.COD_LOJA = ZEUS.COD_LOJA AND INTRANET.COD_AJUSTE = ZEUS.COD_AJUSTE AND INTRANET.COD_PRODUTO = ZEUS.COD_PRODUTO AND  INTRANET.DTA_AJUSTE = ZEUS.DTA_AJUSTE
--				WHERE 1=1
--				AND CAST(CASE WHEN TIPO_OPERACAO = 1 THEN  QTDE_AJUSTE *-1 
--						WHEN TIPO_OPERACAO = 2 THEN QTDE_AJUSTE *-1
--				ELSE QTDE_AJUSTE END -  ISNULL(QTD_AJUSTE,0) AS DECIMAL) <> 0
	

--		END
--ELSE
--		BEGIN

--				UPDATE ZEUS
--				SET ZEUS.QTD_AJUSTE = CASE WHEN TIPO_OPERACAO = 1 THEN  QTDE_AJUSTE *-1 WHEN TIPO_OPERACAO = 2 THEN QTDE_AJUSTE *-1	ELSE QTDE_AJUSTE END 				
--				SELECT *
--				FROM #TEMP_AJUSTE_INTRANET AS INTRANET 
--				LEFT JOIN ZEUS_RTG.DBO.TAB_TIPO_AJUSTE TIPO
--						ON TIPO.COD_AJUSTE = INTRANET.COD_AJUSTE
--				LEFT JOIN ZEUS_RTG.DBO.TAB_AJUSTE_ESTOQUE  AS ZEUS 
--						ON INTRANET.COD_LOJA = ZEUS.COD_LOJA AND INTRANET.COD_AJUSTE = ZEUS.COD_AJUSTE AND INTRANET.COD_PRODUTO = ZEUS.COD_PRODUTO AND  INTRANET.DTA_AJUSTE = ZEUS.DTA_AJUSTE
--				WHERE 1=1
--				AND	CAST(CASE WHEN TIPO_OPERACAO = 1 THEN  QTDE_AJUSTE *-1 
--						WHEN TIPO_OPERACAO = 2 THEN QTDE_AJUSTE *-1
--				ELSE QTDE_AJUSTE END - QTD_AJUSTE AS DECIMAL) <> 0

--		END



SELECT 
		CASE A.COD_LOJA WHEN 4 THEN 'Cotoxo' WHEN 10 THEN 'SCS' WHEN 19 THEN 'Sumare' WHEN 28 THEN 'Orbis' ELSE [NO_LOJA] END AS Loja
		,ISNULL(Qtde,0) AS Qtde
FROM 
		[192.168.0.13].[BI].[DBO].[BI_CAD_LOJA2] AS A 
LEFT JOIN 
			(SELECT 
					INTRANET.[COD_LOJA] as COD_LOJA, 
					COUNT(INTRANET.COD_LOJA) as Qtde 
			FROM #TEMP_AJUSTE_INTRANET AS INTRANET 
				LEFT JOIN ZEUS_RTG.DBO.TAB_TIPO_AJUSTE TIPO
						ON TIPO.COD_AJUSTE = INTRANET.COD_AJUSTE
				LEFT JOIN #TEMP_AJUSTE_EFETIVADOS_ZEUS AS ZEUS 
						ON INTRANET.COD_LOJA = ZEUS.COD_LOJA AND INTRANET.COD_AJUSTE = ZEUS.COD_AJUSTE AND INTRANET.COD_PRODUTO = ZEUS.COD_PRODUTO AND  INTRANET.DTA_AJUSTE = ZEUS.DTA_AJUSTE
				WHERE 1=1
				AND CAST(CASE WHEN TIPO_OPERACAO = 1 THEN  QTDE_AJUSTE *-1 
						WHEN TIPO_OPERACAO = 2 THEN QTDE_AJUSTE *-1
				ELSE QTDE_AJUSTE END -  ISNULL(QTD_AJUSTE,0) AS DECIMAL) <> 0
			GROUP BY INTRANET.COD_LOJA
			) AS B 
				ON B.COD_LOJA = A.COD_LOJA
WHERE 1=1 
AND A.COD_LOJA IN (`1`)	



		
";
		r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLDateTime@dtIni}];
		r
];
r6\[LeftArrow]getQuebras[$lojas,$dataIni]


getCmvCalc[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 

SET NOCOUNT ON;

SELECT DISTINCT COD_LOJA, MAX(CONVERT(DATE,DTA_CALCULO)) ULT_CALC, CONVERT(DATE,GETDATE()-1)ONTEM INTO #TEMP_CMV FROM CMV WHERE 1=1  GROUP BY COD_LOJA 
SELECT DISTINCT COD_LOJA, 'Calculo Ok' A  INTO #TEMP_CMV2 FROM CMV WHERE 1=1  AND DTA_CALCULO = CONVERT(DATE,GETDATE()-1)

SELECT 
		A.COD_LOJA
		,CASE A.COD_LOJA WHEN 4 THEN 'Cotoxo' WHEN 10 THEN 'SCS' WHEN 19 THEN 'Sumare' WHEN 28 THEN 'Orbis' ELSE [NO_LOJA] END AS Loja
		,ISNULL(CAST(A AS VARCHAR),'Sem Calculo') AS [Calc Ontem]
		,ISNULL(CONVERT(VARCHAR,ULT_CALC,103),'Sem Registro') As [Data Ult Calc]
INTO #TEMP_RESUMO
FROM [BI].[DBO].[BI_CAD_LOJA2] AS A LEFT JOIN #TEMP_CMV2 AS B ON B.COD_LOJA = A.COD_LOJA LEFT join #TEMP_CMV C ON C.COD_LOJA = A.COD_LOJA
WHERE 1=1
		 AND A.COD_LOJA NOT IN (19,10,4)
		 AND A.COD_LOJA IN (`1`)
UNION
SELECT 9999 as 'Total', 'Total' as Total, '' as Total, '' as Total
ORDER BY A.COD_LOJA
		 
SELECT Loja, [Calc Ontem], [Data Ult Calc] FROM #TEMP_RESUMO	 
		 
";
		r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
		r
];
r9\[LeftArrow]getCmvCalc[$lojas]


getCalcEstoque[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 
SET NOCOUNT ON;

SELECT COD_LOJA, MAX(CONVERT(DATE,DATA)) AS ULT_CALC INTO #TEMP_EST FROM ESTOQUE GROUP BY COD_LOJA
SELECT DISTINCT COD_LOJA, 'Calculo Ok' A  INTO #TEMP_EST_HJ FROM ESTOQUE WHERE 1=1  AND DATA = CONVERT(DATE,GETDATE())

SELECT 
		A.COD_LOJA
		,CASE A.COD_LOJA WHEN 4 THEN 'Cotoxo' WHEN 10 THEN 'SCS' WHEN 19 THEN 'Sumare' WHEN 28 THEN 'Orbis' ELSE [NO_LOJA] END AS Loja
		,ISNULL(CONVERT(VARCHAR,A,103),'Sem Calculo') AS [Calc Hoje]
		,ISNULL(CONVERT(VARCHAR,ULT_CALC,103),'Sem Registro') As [Data Ult Calc]
INTO #TEMP_RESUMO2
FROM [BI].[DBO].[BI_CAD_LOJA2] AS A LEFT JOIN #TEMP_EST AS B ON B.COD_LOJA = A.COD_LOJA LEFT join #TEMP_EST_HJ C ON C.COD_LOJA = A.COD_LOJA
WHERE 1=1
		 AND A.COD_LOJA NOT IN (19,10,4)
		 AND A.COD_LOJA IN (`1`)
UNION
SELECT 9999 as 'Total', 'Total' as Total, '' as Total, '' as Total		 
ORDER BY A.COD_LOJA		 
		 
SELECT Loja, [Calc Hoje], [Data Ult Calc] FROM #TEMP_RESUMO2	 	 

		 
";
		r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
		r
];
r10\[LeftArrow]getCalcEstoque[$lojas]


(* ::Subtitle:: *)
(*GERANDO GRID*)


grid4[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
	 #<=0,Darker@Green
	,True,Red
	]&;

    stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="0"; 

	grid=maReportGrid[r,"ColumnFormat"->{{"Qtde"}-> stl},"TotalRow"->True,ItemSize->{{6,3}}];
	title=Style[Row@{"Valida\[CCedilla]\[ATilde]o Inv"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
grid4[r4];


grid6[data_Symbol]:=Module[{grid,title,r,color2,stl,total},
	r\[LeftArrow]data;

	   total={"Total",Total@r[[All,"Qtde"]]};
       r["DataAll"]=Join[r["DataAll"],{total}];
	
	color2=Which[ 
	 #<=0,Darker@Green
	,True,Red
	]&;

    stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="0"; 

	grid=maReportGrid[r,"ColumnFormat"->{{"Qtde"}-> stl},"TotalRow"->True,ItemSize->{{6,3}}];
	title=Style[Row@{"Valida\[CCedilla]\[ATilde]o Quebra"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
grid6[r6];


gridUltCalcCmv[data_Symbol]:=Module[{grid,title,r,color2,stl,color3,stl3,colorTest},
	r\[LeftArrow]data;
	color2=Which[			
			#==="Sem Registro",Red
			,True,Darker@Green 
	]&;

	stl[val_]:=Style[maF["Date"][val],Bold,color2@val];
	stl[Null]="OK";

	color3=Which[			
			#==="Calculo Ok",Darker@Green
			,True,Red
	]&;

	stl3[val_]:= Style[maF["Date"][val],Bold,color3@val];
	stl3[Null]="OK";

	grid=maReportGrid[r,"ColumnFormat"->{{"Data Ult Calc"}-> stl, {"Calc Ontem"}-> stl3},"TotalRow"->True];
	title=Style[Row@{"\[CapitalUAcute]ltimo C\[AAcute]lculo CMV"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
gridUltCalcCmv[r9];


gridCalcEstoque[data_Symbol]:=Module[{grid,title,r,color2,stl,color3,stl3,colorTest},
	r\[LeftArrow]data;
	color2=Which[			
			#==="Sem Registro",Red
			,True,Darker@Green 
	]&;

	stl[val_]:=Style[maF["Date"][val],Bold,color2@val];
	stl[Null]="OK";

	color3=Which[			
			#==="Calculo Ok",Darker@Green
			,True,Red
	]&;

	stl3[val_]:= Style[maF["Date"][val],Bold,color3@val];
	stl3[Null]="OK";

	grid=maReportGrid[r,"ColumnFormat"->{{"Data Ult Calc"}-> stl, {"Calc Hoje"}-> stl3},"TotalRow"->True];
	title=Style[Row@{"\[CapitalUAcute]ltimo C\[AAcute]lculo Estoque"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
gridCalcEstoque[r10];


createReport1[data_Symbol]:=Module[{r},
	r\[LeftArrow]data;
	Grid[{{grid4[r4],grid6[r6],gridCalcEstoque[r10],gridUltCalcCmv[r9]}},Spacings->{0.5,4},Frame->Transparent]
] 
createReport1[r100];
$Dash1=marcheTemplate[grid=Row@{"DashBoard N\.ba1 - Fluxo de movimeta\[CCedilla]\[OTilde]es de estoque em ", $now},createReport1[r100],1200, $logo];
$Dash1=Export["DashBoard N\.ba1 - Fluxo de movimeta\[CCedilla]\[OTilde]es de estoque.png",$Dash1];


\[AliasDelimiter]


(* ::Title:: *)
(*ENVIANDO EMAIL*)


$SqlInv=Import["Inv.txt"];
$SqlQuebra=Import["Quebras.txt"];

$Inv=Export["Query Inv.txt",$SqlInv];
$Quebras=Export["Query Quebra.txt",$SqlQuebra];


marcheMail[
  		"[GE] Fluxo de movimeta\[CCedilla]\[OTilde]es de estoque em " <>$DateOntem
  		,"Analise desde "<>$dataIniAnalise
  		,$mailsGerencia
		  ,{$Dash1,$Inv,$Quebras}
]
