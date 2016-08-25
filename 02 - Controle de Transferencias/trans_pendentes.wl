(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$lojas={1,2,3,6,7,9,12,13,17,18,20,21,22,23,24,25,27,28,29,30,31,33,9999};
$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$mails=marcheGetMails[];
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["Marche.png"];


getPENDENCIAS[loja1_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 

SET NOCOUNT ON;


SELECT 
		COD_LOJA
		,NO_LOJA
INTO #LOJA_TEMP
FROM [192.168.0.13].[BI].[DBO].[BI_CAD_LOJA2]
WHERE 1=1 AND COD_LOJA IN (`1`)

		--INSERT INTO #LOJA_TEMP VALUES (99,'REDE')

--PENDENTE--

SELECT 
			T.COD_LOJA
			,COUNT(DISTINCT T.ID) AS PENDENTE 
			,SUM(QTDE*VAL_CUSTO_REP) AS CUSTO

INTO #TEMP_PENDENTE
FROM [INTRANET].[DBO].[TAB_TRANSFINTERNAS] AS T 
INNER JOIN [INTRANET].[DBO].[TAB_TRANSFINTERNAS_PROD] AS P 
		ON T.ID =P.IDTRANSF
LEFT JOIN ZEUS_RTG.DBO.TAB_CODIGO_BARRA AS ZEUS_BARRA (NOLOCK) 
		ON CONVERT(NUMERIC,(P.COD_EAN)) =  CONVERT(NUMERIC,ZEUS_BARRA.COD_EAN)
LEFT JOIN TAB_PRODUTO_LOJA AS CUSTO 
		ON CUSTO.COD_LOJA = T.COD_LOJA AND CUSTO.COD_PRODUTO = ZEUS_BARRA.COD_PRODUTO 
WHERE 1=1	
			AND T.COD_LOJA IN (`1`)	
			AND T.STATUS = 0
			AND DATA < CONVERT(DATE,GETDATE())
			GROUP BY T.COD_LOJA


--FINALIZADO--
SELECT 
			T.COD_LOJA
			,COUNT(DISTINCT T.ID) AS FINALIZADO 
			,SUM(QTDE*VAL_CUSTO_REP) AS CUSTO
INTO  #TEMP_FINALIZADO
FROM [INTRANET].[DBO].[TAB_TRANSFINTERNAS] AS T 
INNER JOIN [INTRANET].[DBO].[TAB_TRANSFINTERNAS_PROD] AS P 
		ON T.ID =P.IDTRANSF
LEFT JOIN ZEUS_RTG.DBO.TAB_CODIGO_BARRA AS ZEUS_BARRA (NOLOCK) 
		ON CONVERT(NUMERIC,(P.COD_EAN)) =  CONVERT(NUMERIC,ZEUS_BARRA.COD_EAN)
LEFT JOIN TAB_PRODUTO_LOJA AS CUSTO 
		ON CUSTO.COD_LOJA = T.COD_LOJA AND CUSTO.COD_PRODUTO = ZEUS_BARRA.COD_PRODUTO 

WHERE 1=1
			AND T.COD_LOJA IN (`1`)
			AND T.STATUS = 6
			AND DATA < CONVERT(DATE,GETDATE())
			GROUP BY T.COD_LOJA

--LAST DATE--		
SELECT	
			COD_LOJA
			,MIN(CONVERT(DATE,DATA)) AS LAST_DATE
			INTO #TEMP_LAST_DATE
FROM [INTRANET].[DBO].[TAB_TRANSFINTERNAS] AS TRANS 
			WHERE 1=1
			AND COD_LOJA IN (`1`)
			AND DATA < CONVERT(DATE,GETDATE())
			AND TRANS.STATUS IN (0,6)
			GROUP BY COD_LOJA			


--SOMA--		
SELECT 
			 LJ.COD_LOJA
			,LJ.NO_LOJA AS NO_LOJA
			,ISNULL (FINALIZADO,0) AS FINALIZADO
			,ISNULL (PENDENTE,0) AS PENDENTE
			,ISNULL(F.CUSTO,0)+ISNULL(P.CUSTO,0) VLR_TOTAL
			,ISNULL (CONVERT(VARCHAR,LAST_DATE,103),'OK') AS LAST_DATE
			INTO #TEMP_TOTAL
FROM #LOJA_TEMP AS LJ
			LEFT JOIN #TEMP_FINALIZADO AS F ON F.COD_LOJA = LJ.COD_LOJA
			LEFT JOIN #TEMP_PENDENTE AS P ON P.COD_LOJA = LJ.COD_LOJA
			LEFT JOIN #TEMP_LAST_DATE AS L ON L.COD_LOJA = LJ.COD_LOJA
		
SELECT DISTINCT COD_LOJA, DATA AS DTA_REGISTRO, DTAPROVACAO AS DTA_APROVACAO  INTO #TEMP_MEDIA FROM [INTRANET].[DBO].[TAB_TRANSFINTERNAS] WHERE 1=1 AND STATUS = 7 AND DTAPROVACAO IS NOT NULL AND DTAPROVACAO >= CONVERT(DATE,GETDATE()-15) AND COD_LOJA IN (`1`)
SELECT *,CAST(DTA_APROVACAO-DTA_REGISTRO AS DOUBLE PRECISION) AS DIAS_PARA_APROVAR INTO #TEMP_MEDIA2 FROM #TEMP_MEDIA
SELECT COD_LOJA , AVG(DIAS_PARA_APROVAR) AS MEDIA INTO #MEDIA_APROVACAO FROM #TEMP_MEDIA2 GROUP BY COD_LOJA ORDER BY COD_LOJA

SELECT			
			A.COD_LOJA
			,NO_LOJA AS LOJA
			,FINALIZADO AS 'APROV. GERENTE'
			,PENDENTE AS 'FINALIZA\[CapitalCCedilla]\[CapitalATilde]O LOJA'
			,PENDENTE+FINALIZADO AS TOTAL
			,VLR_TOTAL [VLR PARCIAL]
			,LAST_DATE AS 'PENDENTE DESDE'
			,ISNULL(MEDIA,0) AS 'MEDIA (DIAS)'
INTO #TMP_RESUMO
FROM #TEMP_TOTAL A LEFT JOIN #MEDIA_APROVACAO AS B ON A.COD_LOJA = B.COD_LOJA
UNION
SELECT 9999 , 'Total' , SUM(FINALIZADO)  , SUM(PENDENTE)  , SUM(FINALIZADO+PENDENTE) ,SUM(VLR_TOTAL) , '' , (SELECT AVG(DIAS_PARA_APROVAR) AS MEDIA FROM #TEMP_MEDIA2)  FROM   #TEMP_TOTAL 
ORDER BY A.COD_LOJA 

SELECT 
		LOJA Loja
		,[APROV. GERENTE] as [Ap. Gerente]
		,[FINALIZA\[CapitalCCedilla]\[CapitalATilde]O LOJA] as [Finaliza\[CCedilla]\[ATilde]o Loja]
		,TOTAL Total
		,[VLR PARCIAL] As Custo
		,[PENDENTE DESDE] as [Pendente Desde]
		,[MEDIA (Dias)] as M\[EAcute]dia
 FROM #TMP_RESUMO
 WHERE 1=1 
		AND COD_LOJA IN (`1`)


DROP TABLE #TEMP_TOTAL
DROP TABLE #TEMP_FINALIZADO
DROP TABLE #TEMP_PENDENTE
DROP TABLE #TEMP_LAST_DATE
DROP TABLE #LOJA_TEMP
DROP TABLE #TEMP_MEDIA
DROP TABLE #TEMP_MEDIA2
DROP TABLE #MEDIA_APROVACAO
DROP TABLE #TMP_RESUMO

";

r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@loja1}];
r

];
r\[LeftArrow]getPENDENCIAS[$lojas]


gridPENDENCIAS[data_Symbol]:=Module[{grid,title,r,color2,stl,color3,stl3,colorTest},
	r\[LeftArrow]data;
	color2=Which[ 
			 #>=1,Red
			,True,Darker@Green
	]&;

	stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="OK";

	color3=Which[			
			#==="OK",Darker@Green
			,True,Red
	]&;

	stl3[val_]:= Style[maF["Date"][val],Bold,color3@val];
	stl3[Null]="OK";
	
	grid=maReportGrid[r,"ColumnFormat"->{{"Ap. Gerente","Finaliza\[CCedilla]\[ATilde]o Loja","Total","Custo","M\[EAcute]dia"}-> stl, {"Pendente Desde"}-> stl3},"TotalRow"->True];
	title=Style[Row@{"Aprova\[CCedilla]\[OTilde]es Pendentes por Loja"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
gridPENDENCIAS[r]


getPENDENCIASABERTAS[loja1_]:=Module[{sql,c,conn=marcheConn[],tab},
	sql=" 

SET NOCOUNT ON;

SELECT DISTINCT 
		COD_LOJA, 
		MIN(DATA) AS DATA, 
		DES_AJUSTE 
INTO #TEMP_RESUMO 
FROM 
		[INTRANET].[DBO].[TAB_TRANSFINTERNAS] AS INTRA 
LEFT JOIN TAB_TIPO_AJUSTE AS TIPO 
ON INTRA.TIPO = TIPO.COD_AJUSTE 
WHERE 1=1
		AND STATUS IN (0,6)
		AND DATA < CONVERT(DATE,GETDATE())
GROUP BY 
		COD_LOJA, 
		TIPO.DES_AJUSTE
ORDER BY DATA

SELECT TOP 21
		NO_LOJA AS LOJA
		,CONVERT(VARCHAR,DATA,103) AS DATA 
		,DES_AJUSTE AS 'TIPO TRANSFER\[CapitalEHat]NCIA'
FROM #TEMP_RESUMO AS INTRA 
LEFT JOIN [192.168.0.13].[BI].[DBO].[BI_CAD_LOJA2] AS LJ 
		ON INTRA.COD_LOJA = LJ.COD_LOJA
WHERE 1=1
AND INTRA.COD_LOJA IN (`1`)
";

c\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@loja1}];
	c
];
c\[LeftArrow]getPENDENCIASABERTAS[$lojas]


gridPENDENCIASABERTAS[data_Symbol]:=Module[{grid,title,c,color2,stl,color3,stl3,colorTest},
	c\[LeftArrow]data;
	color2=Which[ 
			 #<=0,Darker@Green
			,True,Red
	]&;

	stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="OK";

	color3=Which[			
			#==="OK",Darker@Green
			,True,Red
	]&;

	stl3[val_]:= Style[maF["Date"][val],Bold,color3@val];
	stl3[Null]="OK";
	
	grid=maReportGrid[c,"ColumnFormat"->{{"DATA"}-> stl3},"TotalRow"->False];
	title=Style[Row@{"Top Pend\[EHat]ncias"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])

]
gridPENDENCIASABERTAS[c];


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

createReport[data_Symbol]:=Module[{s},
	s\[LeftArrow]data;
	Grid[{{gridPENDENCIAS[r],gridPENDENCIASABERTAS[c]}},Spacings->{1.5,4},Frame->Transparent]
]
createReport[s];

rep=marcheTemplate[grid=Row@{"Controle Aprova\[CCedilla]\[ATilde]o de Transfer\[EHat]ncias/Quebras em ",$now},createReport[s],1200, $logo]


$fileName="Controle de Transfer\[EHat]ncias.png";
Export["Controle de Transfer\[EHat]ncias.png",rep];


marcheMail[
		"[GE] Controle Aprova\[CCedilla]\[ATilde]o de Transfer\[EHat]ncias/Quebras Di\[AAcute]rio "<>$now
		,"Relat\[OAcute]rio - Controle Aprova\[CCedilla]\[ATilde]o de Transfer\[EHat]ncias/Quebras Di\[AAcute]rio\n\nLegenda:\nAPROV. GERENTE: Aguardando aprova\[CCedilla]\[ATilde]o do Gerente;\nFINALIZA\[CapitalCCedilla]\[CapitalATilde]O LOJA: Lan\[CCedilla]amento n\[ATilde]o finalizado;\nM\[CapitalEAcute]DIA: M\[EAcute]dia de aprova\[CCedilla]\[ATilde]o dos \[UAcute]ltimos 30 dias."
		,$mailsGerencia
		,{$fileName}
];


(*marcheMail[*)
(*		"Controle de transfer\[EHat]ncia di\[AAcute]rio "<>$now*)
(*		,"Controle de transfer\[EHat]ncia di\[AAcute]rio\n\nLegenda:\nAPROV. GERENTE: Aguardando aprova\[CCedilla]\[ATilde]o do Gerente\nFINALIZA\[CapitalCCedilla]\[CapitalATilde]O LOJA: Lan\[CCedilla]amento n\[ATilde]o finalizado"*)
(*		,{"diego.miller@marche.com.br","cleberton.silva@marche.com.br"}*)
(*		,{$fileName}*)
(*];*)
