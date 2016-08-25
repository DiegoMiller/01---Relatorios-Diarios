(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$dtIni={2016,04,04};
$lojas={33};


$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["Marche.png"];


getTOP23[codLoja_,dataIni_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 

SET NOCOUNT ON;	

------------------------------------------------------------------------
--BUSCANDO E CONVERTENDO TACAS E GARRAFAS
------------------------------------------------------------------------
SELECT 
		 A.COD_LOJA
		--,A.COD_PRODUTO
		,COD_PRODUTO_GR COD_PRODUTO
		--,SUM(CAST(QTDE_PRODUTO AS DOUBLE PRECISION)) AS QTD_VENDA_TC
		,SUM(CAST(QTDE_PRODUTO AS INT))/4 AS QTD_VENDA
INTO #TMP_VENDAS
FROM [BI].[DBO].BI_VENDA_PRODUTO A 
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO B 
			ON B.COD_PRODUTO = A.COD_PRODUTO 
		INNER JOIN [BI].[DBO].[CADASTRO_DEPARA_VINHO] AS DEPARA
			ON DEPARA.COD_PRODUTO_TC = A.COD_PRODUTO
WHERE 1=1 
		AND COD_LOJA IN (`1`)
		AND CONVERT(DATE,DATA) >= (`2`)
GROUP BY
		A.COD_LOJA
		,A.COD_PRODUTO
		,[COD_PRODUTO_GR]			
UNION 
SELECT 	 A.COD_LOJA
		,A.COD_PRODUTO
		,SUM(CAST(QTDE_PRODUTO AS DOUBLE PRECISION)) AS QTD_VENDA
FROM [BI].[DBO].BI_VENDA_PRODUTO A 
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO B 
			ON B.COD_PRODUTO = A.COD_PRODUTO 
WHERE 1=1 
		AND COD_LOJA IN (`1`)
		AND CONVERT(DATE,DATA) >= (`2`)
		AND COD_DEPARTAMENTO IN (2)	
GROUP BY
		A.COD_LOJA
	   ,A.COD_PRODUTO
------------------------------------------------------------------------
--SOMANDO GARRAFAS
------------------------------------------------------------------------
SELECT 
		COD_PRODUTO,SUM(QTD_VENDA) AS QTD_VENDA
INTO #TEMP_VENDA_ITENS
FROM #TMP_VENDAS VENDAS

WHERE 1=1 
GROUP BY 
		COD_PRODUTO
------------------------------------------------------------------------
--TRANSF--
------------------------------------------------------------------------	
SELECT 		
		 AJUSTE.COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_TRANSF
INTO  #TAB_TRANSF
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK)
WHERE 1=1
		AND COD_AJUSTE IN (269)
		AND COD_LOJA IN (29)
		AND CONVERT(DATE,DTA_AJUSTE) >= (`2`)
GROUP BY 
		AJUSTE.COD_PRODUTO			
------------------------------------------------------------------------
--JUNTANDO VENDAS E TRANSFERENCIAS
------------------------------------------------------------------------		
SELECT
		 VENDAS.COD_PRODUTO
		,SUM(QTD_VENDA) AS QTD_VENDA
		,ISNULL(SUM(QTD_TRANSF),0) AS QTD_TRANSF 
INTO #TMP_VENDA_X_TRANSF
FROM #TEMP_VENDA_ITENS VENDAS 
	LEFT JOIN #TAB_TRANSF TRANSF 
			ON TRANSF.COD_PRODUTO = VENDAS.COD_PRODUTO
WHERE 1=1 
		AND QTD_VENDA > 0
GROUP BY 
		VENDAS.COD_PRODUTO
------------------------------------------------------------------------
--BUSCANDO TRANSFERENCIAS PENDENTES PARA DESCONTAR DO CALCULO
------------------------------------------------------------------------		
SELECT  ZEUS_BARRA.COD_PRODUTO COD_PRODUTO
	    ,SUM(CAST (QTDE AS DOUBLE PRECISION)) AS QTDE_PENDENCIA
INTO #TMP_INTRANET_PENDENTES
FROM [192.168.0.6].[INTRANET].[DBO].[TAB_TRANSFINTERNAS] AS T 
	INNER JOIN [192.168.0.6].[INTRANET].[DBO].[TAB_TRANSFINTERNAS_PROD] AS P 
			ON T.ID =P.IDTRANSF
	LEFT JOIN [192.168.0.6].ZEUS_RTG.DBO.TAB_CODIGO_BARRA AS ZEUS_BARRA (NOLOCK) 		
			ON CONVERT(NUMERIC,(P.COD_EAN)) =  CONVERT(NUMERIC,ZEUS_BARRA.COD_EAN)
WHERE 1=1
		AND T.STATUS IN (0,6)
		AND TIPO = 269 
		AND T.COD_LOJA = 29 
		AND ZEUS_BARRA.COD_PRODUTO  IN (SELECT COD_PRODUTO FROM #TMP_VENDA_X_TRANSF)
		AND ZEUS_BARRA.COD_EAN NOT IN (' ')
GROUP BY 
		ZEUS_BARRA.COD_PRODUTO

------------------------------------------------------------------------
--TOP 22
------------------------------------------------------------------------
SELECT TOP 23
		A.COD_PRODUTO Plu
		,Descricao 'Descri\[CCedilla]\[ATilde]o'
		,NO_DEPARTAMENTO Departamento
		,QTD_VENDA [Qtde Venda]
		,QTD_TRANSF-ISNULL(QTDE_PENDENCIA,0) [Qtde Transf]
		,QTD_VENDA-QTD_TRANSF-ISNULL(QTDE_PENDENCIA,0)  [\[CapitalAGrave] Transf]
FROM #TMP_VENDA_X_TRANSF A 
	LEFT JOIN #TMP_INTRANET_PENDENTES B 
		ON B.COD_PRODUTO = A.COD_PRODUTO
	LEFT JOIN  BI.DBO.BI_CAD_PRODUTO C 
		ON C.COD_PRODUTO = A.COD_PRODUTO 
 WHERE 1=1
		AND QTD_VENDA-QTD_TRANSF-ISNULL(QTDE_PENDENCIA,0) >0
ORDER BY 
		QTD_VENDA-QTD_TRANSF-ISNULL(QTDE_PENDENCIA,0) DESC
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLDateTime@dataIni}];
	r
];
r2\[LeftArrow]getTOP23[$lojas,$dtIni]


grid8[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
	 #<=0,Darker@Green
	,True,Red
	]&;

    stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="0"; 

	grid=maReportGrid[r,"ColumnFormat"->{{"\[CapitalAGrave] Transf"}-> stl,{"Qtde Venda","Qtde Transf"}-> "N"},"TotalRow"->False];
	title=Style[Row@{"Top Itens"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
grid8[r2];


getTOTAL[codLoja_,dataIni_]:=Module[{sql,s,conn=marcheConn2[],tab},
	sql=" 

SET NOCOUNT ON;	

------------------------------------------------------------------------
--BUSCANDO E CONVERTENDO TACAS E GARRAFAS
------------------------------------------------------------------------
SELECT 
		 A.COD_LOJA
		--,A.COD_PRODUTO
		,COD_PRODUTO_GR COD_PRODUTO
		--,SUM(CAST(QTDE_PRODUTO AS DOUBLE PRECISION)) AS QTD_VENDA_TC
		,SUM(CAST(QTDE_PRODUTO AS INT))/4 AS QTD_VENDA
INTO #TMP_VENDAS
FROM [BI].[DBO].BI_VENDA_PRODUTO A 
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO B 
			ON B.COD_PRODUTO = A.COD_PRODUTO 
		INNER JOIN [BI].[DBO].[CADASTRO_DEPARA_VINHO] AS DEPARA
			ON DEPARA.COD_PRODUTO_TC = A.COD_PRODUTO
WHERE 1=1 
		AND COD_LOJA IN (`1`)
		AND CONVERT(DATE,DATA) >= (`2`)
GROUP BY
		A.COD_LOJA
		,A.COD_PRODUTO
		,[COD_PRODUTO_GR]			
UNION 
SELECT 	 A.COD_LOJA
		,A.COD_PRODUTO
		,SUM(CAST(QTDE_PRODUTO AS DOUBLE PRECISION)) AS QTD_VENDA
FROM [BI].[DBO].BI_VENDA_PRODUTO A 
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO B 
			ON B.COD_PRODUTO = A.COD_PRODUTO 
WHERE 1=1 
		AND COD_LOJA IN (`1`)
		AND CONVERT(DATE,DATA) >= (`2`)
		AND COD_DEPARTAMENTO IN (2)	
GROUP BY
		A.COD_LOJA
	   ,A.COD_PRODUTO
------------------------------------------------------------------------
--SOMANDO GARRAFAS
------------------------------------------------------------------------
SELECT 
		COD_PRODUTO,SUM(QTD_VENDA) AS QTD_VENDA
INTO #TEMP_VENDA_ITENS
FROM #TMP_VENDAS VENDAS

WHERE 1=1 
GROUP BY 
		COD_PRODUTO
------------------------------------------------------------------------
--TRANSF--
------------------------------------------------------------------------	
SELECT 		
		 AJUSTE.COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_TRANSF
INTO  #TAB_TRANSF
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK)
WHERE 1=1
		AND COD_AJUSTE IN (269)
		AND COD_LOJA IN (29)
		AND CONVERT(DATE,DTA_AJUSTE) >= (`2`)
GROUP BY 
		AJUSTE.COD_PRODUTO			
------------------------------------------------------------------------
--JUNTANDO VENDAS E TRANSFERENCIAS
------------------------------------------------------------------------		
SELECT
		 VENDAS.COD_PRODUTO
		,SUM(QTD_VENDA) AS QTD_VENDA
		,ISNULL(SUM(QTD_TRANSF),0) AS QTD_TRANSF 
INTO #TMP_VENDA_X_TRANSF
FROM #TEMP_VENDA_ITENS VENDAS 
	LEFT JOIN #TAB_TRANSF TRANSF 
			ON TRANSF.COD_PRODUTO = VENDAS.COD_PRODUTO
WHERE 1=1 
		AND QTD_VENDA > 0
GROUP BY 
		VENDAS.COD_PRODUTO
------------------------------------------------------------------------
--BUSCANDO TRANSFERENCIAS PENDENTES PARA DESCONTAR DO CALCULO
------------------------------------------------------------------------		
SELECT  ZEUS_BARRA.COD_PRODUTO COD_PRODUTO
	    ,SUM(CAST (QTDE AS DOUBLE PRECISION)) AS QTDE_PENDENCIA
INTO #TMP_INTRANET_PENDENTES
FROM [192.168.0.6].[INTRANET].[DBO].[TAB_TRANSFINTERNAS] AS T 
	INNER JOIN [192.168.0.6].[INTRANET].[DBO].[TAB_TRANSFINTERNAS_PROD] AS P 
			ON T.ID =P.IDTRANSF
	LEFT JOIN [192.168.0.6].ZEUS_RTG.DBO.TAB_CODIGO_BARRA AS ZEUS_BARRA (NOLOCK) 		
			ON CONVERT(NUMERIC,(P.COD_EAN)) =  CONVERT(NUMERIC,ZEUS_BARRA.COD_EAN)
WHERE 1=1
		AND T.STATUS IN (0,6)
		AND TIPO = 269 
		AND T.COD_LOJA = 29 
		AND ZEUS_BARRA.COD_PRODUTO  IN (SELECT COD_PRODUTO FROM #TMP_VENDA_X_TRANSF)
		AND ZEUS_BARRA.COD_EAN NOT IN (' ')
GROUP BY 
		ZEUS_BARRA.COD_PRODUTO

------------------------------------------------------------------------
--DADOS PARA EXCEL
------------------------------------------------------------------------
SELECT 
		A.COD_PRODUTO Plu
		,Descricao
		,NO_DEPARTAMENTO Departamento
		,QTD_VENDA-QTD_TRANSF-ISNULL(QTDE_PENDENCIA,0)  [\[CapitalAGrave] Transf]
INTO #TMP_EXCEL
FROM #TMP_VENDA_X_TRANSF A 
	LEFT JOIN #TMP_INTRANET_PENDENTES B 
		ON B.COD_PRODUTO = A.COD_PRODUTO
	LEFT JOIN  BI.DBO.BI_CAD_PRODUTO C 
		ON C.COD_PRODUTO = A.COD_PRODUTO 
 WHERE 1=1
		AND QTD_VENDA-QTD_TRANSF-ISNULL(QTDE_PENDENCIA,0) >0
ORDER BY 
		QTD_VENDA-QTD_TRANSF-ISNULL(QTDE_PENDENCIA,0) DESC
------------------------------------------------------------------------
--DEPARTAMENTO
------------------------------------------------------------------------
SELECT DISTINCT NO_DEPARTAMENTO INTO #TEMP_CAD FROM BI.DBO.BI_CAD_PRODUTO

SELECT
		NO_DEPARTAMENTO 
		,COUNT(A.Plu) as Qtde
INTO #TMP_DEPARTAMENTO
FROM #TMP_EXCEL A 
	LEFT JOIN BI.DBO.BI_CAD_PRODUTO B 
		ON B.COD_PRODUTO = A.PLU 
GROUP BY 
		NO_DEPARTAMENTO
							
SELECT A.NO_DEPARTAMENTO AS Departamento, ISNULL(Qtde,0) as Qtde  FROM  #TEMP_CAD AS A LEFT JOIN #TMP_DEPARTAMENTO AS B ON A.NO_DEPARTAMENTO = B.NO_DEPARTAMENTO
UNION		
SELECT 'TOTAL' as TOTAL, SUM(QTDE)as QTDE FROM  #TMP_DEPARTAMENTO

 ";
		s\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLDateTime@dataIni}];
		s
];
s\[LeftArrow]getTOTAL[$lojas,$dtIni]


gridTOTAL[data_Symbol]:=Module[{grid,title,s,color2,stl},
	s\[LeftArrow]data;
	color2=Which[ 
	 #<=0,Darker@Green
	,True,Red
	]&;

	stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="";

	grid=maReportGrid[s,"ColumnFormat"->{{"Qtde"}-> stl},"TotalRow"->True];
	title=Style[Row@{"Departamento"},maTitleFormatOptions];
	(maReportFrame[title,grid])
]
gridTOTAL[s];


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

createReport[data_Symbol]:=Module[{s},
	s\[LeftArrow]data;
	Grid[{{gridTOTAL[s],grid8[r2]}},Spacings->{1.5,4},Frame->Transparent]
]
createReport[s];
rep=marcheTemplate[grid=Row@{"Pend\[EHat]ncias de Transfer\[EHat]ncias Estoque Restaurante 29-Eataly para 33-Eataly em ",$now},createReport[s],1200, $logo];

$fileName="Pend\[EHat]ncias de Transferencias 29-Eataly para 33-Eataly Restaurante em "<>$now2<>".png";
Export["Pend\[EHat]ncias de Transferencias 29-Eataly para 33-Eataly Restaurante em "<>$now2<>".png",rep];


getEXCEL[codLoja_,dataIni_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 

SET NOCOUNT ON;	

------------------------------------------------------------------------
--BUSCANDO E CONVERTENDO TACAS E GARRAFAS
------------------------------------------------------------------------
SELECT 
		 A.COD_LOJA
		--,A.COD_PRODUTO
		,COD_PRODUTO_GR COD_PRODUTO
		--,SUM(CAST(QTDE_PRODUTO AS DOUBLE PRECISION)) AS QTD_VENDA_TC
		,SUM(CAST(QTDE_PRODUTO AS INT))/4 AS QTD_VENDA
INTO #TMP_VENDAS
FROM [BI].[DBO].BI_VENDA_PRODUTO A 
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO B 
			ON B.COD_PRODUTO = A.COD_PRODUTO 
		INNER JOIN [BI].[DBO].[CADASTRO_DEPARA_VINHO] AS DEPARA
			ON DEPARA.COD_PRODUTO_TC = A.COD_PRODUTO
WHERE 1=1 
		AND COD_LOJA IN (`1`)
		AND CONVERT(DATE,DATA) >= (`2`)
GROUP BY
		A.COD_LOJA
		,A.COD_PRODUTO
		,[COD_PRODUTO_GR]			
UNION 
SELECT 	 A.COD_LOJA
		,A.COD_PRODUTO
		,SUM(CAST(QTDE_PRODUTO AS DOUBLE PRECISION)) AS QTD_VENDA
FROM [BI].[DBO].BI_VENDA_PRODUTO A 
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO B 
			ON B.COD_PRODUTO = A.COD_PRODUTO 
WHERE 1=1 
		AND COD_LOJA IN (`1`)
		AND CONVERT(DATE,DATA) >= (`2`)
		AND COD_DEPARTAMENTO IN (2)	
GROUP BY
		A.COD_LOJA
	   ,A.COD_PRODUTO
------------------------------------------------------------------------
--SOMANDO GARRAFAS
------------------------------------------------------------------------
SELECT 
		COD_PRODUTO,SUM(QTD_VENDA) AS QTD_VENDA
INTO #TEMP_VENDA_ITENS
FROM #TMP_VENDAS VENDAS

WHERE 1=1 
GROUP BY 
		COD_PRODUTO
------------------------------------------------------------------------
--TRANSF--
------------------------------------------------------------------------	
SELECT 		
		 AJUSTE.COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_TRANSF
INTO  #TAB_TRANSF
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK)
WHERE 1=1
		AND COD_AJUSTE IN (269)
		AND COD_LOJA IN (29)
		AND CONVERT(DATE,DTA_AJUSTE) >= (`2`)
GROUP BY 
		AJUSTE.COD_PRODUTO			
------------------------------------------------------------------------
--JUNTANDO VENDAS E TRANSFERENCIAS
------------------------------------------------------------------------		
SELECT
		 VENDAS.COD_PRODUTO
		,SUM(QTD_VENDA) AS QTD_VENDA
		,ISNULL(SUM(QTD_TRANSF),0) AS QTD_TRANSF 
INTO #TMP_VENDA_X_TRANSF
FROM #TEMP_VENDA_ITENS VENDAS 
	LEFT JOIN #TAB_TRANSF TRANSF 
			ON TRANSF.COD_PRODUTO = VENDAS.COD_PRODUTO
WHERE 1=1 
		AND QTD_VENDA > 0
GROUP BY 
		VENDAS.COD_PRODUTO
------------------------------------------------------------------------
--BUSCANDO TRANSFERENCIAS PENDENTES PARA DESCONTAR DO CALCULO
------------------------------------------------------------------------		
SELECT  ZEUS_BARRA.COD_PRODUTO COD_PRODUTO
	    ,SUM(CAST (QTDE AS DOUBLE PRECISION)) AS QTDE_PENDENCIA
INTO #TMP_INTRANET_PENDENTES
FROM [192.168.0.6].[INTRANET].[DBO].[TAB_TRANSFINTERNAS] AS T 
	INNER JOIN [192.168.0.6].[INTRANET].[DBO].[TAB_TRANSFINTERNAS_PROD] AS P 
			ON T.ID =P.IDTRANSF
	LEFT JOIN [192.168.0.6].ZEUS_RTG.DBO.TAB_CODIGO_BARRA AS ZEUS_BARRA (NOLOCK) 		
			ON CONVERT(NUMERIC,(P.COD_EAN)) =  CONVERT(NUMERIC,ZEUS_BARRA.COD_EAN)
WHERE 1=1
		AND T.STATUS IN (0,6)
		AND TIPO = 269 
		AND T.COD_LOJA = 29 
		AND ZEUS_BARRA.COD_PRODUTO  IN (SELECT COD_PRODUTO FROM #TMP_VENDA_X_TRANSF)
		AND ZEUS_BARRA.COD_EAN NOT IN (' ')
GROUP BY 
		ZEUS_BARRA.COD_PRODUTO

------------------------------------------------------------------------
--DADOS PARA EXCEL
------------------------------------------------------------------------
SELECT 
		A.COD_PRODUTO Plu
		,Descricao
		,NO_DEPARTAMENTO Departamento
		,QTD_VENDA-QTD_TRANSF-ISNULL(QTDE_PENDENCIA,0)  [\[CapitalAGrave] Transf]
FROM #TMP_VENDA_X_TRANSF A 
	LEFT JOIN #TMP_INTRANET_PENDENTES B 
		ON B.COD_PRODUTO = A.COD_PRODUTO
	LEFT JOIN  BI.DBO.BI_CAD_PRODUTO C 
		ON C.COD_PRODUTO = A.COD_PRODUTO 
 WHERE 1=1
		AND QTD_VENDA-QTD_TRANSF-ISNULL(QTDE_PENDENCIA,0) >0
ORDER BY 
		QTD_VENDA-QTD_TRANSF-ISNULL(QTDE_PENDENCIA,0) DESC

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLDateTime@dataIni}];
	r
];
r4\[LeftArrow]getEXCEL[$lojas,$dtIni]


$fileName2="Pendencia de Transferencia Eataly para Eataly Restaurante - Vinhos em "<>$now2<>".xlsx";
Export["Pendencia de Transferencia Eataly para Eataly Restaurante - Vinhos em "<>$now2<>".xlsx",r4["DataAll"]];		


getEXCELINCORRETOS[codLoja_,dataIni_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 

SET NOCOUNT ON;	

------------------------------------------------------------------------
--BUSCANDO E CONVERTENDO TACAS E GARRAFAS
------------------------------------------------------------------------
SELECT 
		 A.COD_LOJA
		--,A.COD_PRODUTO
		,COD_PRODUTO_GR COD_PRODUTO
		--,SUM(CAST(QTDE_PRODUTO AS DOUBLE PRECISION)) AS QTD_VENDA_TC
		,SUM(CAST(QTDE_PRODUTO AS INT))/4 AS QTD_VENDA
INTO #TMP_VENDAS
FROM [BI].[DBO].BI_VENDA_PRODUTO A 
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO B 
			ON B.COD_PRODUTO = A.COD_PRODUTO 
		INNER JOIN [BI].[DBO].[CADASTRO_DEPARA_VINHO] AS DEPARA
			ON DEPARA.COD_PRODUTO_TC = A.COD_PRODUTO
WHERE 1=1 
		AND COD_LOJA IN (`1`)
		AND CONVERT(DATE,DATA) >= (`2`)
GROUP BY
		A.COD_LOJA
		,A.COD_PRODUTO
		,[COD_PRODUTO_GR]			
UNION 
SELECT 	 A.COD_LOJA
		,A.COD_PRODUTO
		,SUM(CAST(QTDE_PRODUTO AS DOUBLE PRECISION)) AS QTD_VENDA
FROM [BI].[DBO].BI_VENDA_PRODUTO A 
		LEFT JOIN BI.DBO.BI_CAD_PRODUTO B 
			ON B.COD_PRODUTO = A.COD_PRODUTO 
WHERE 1=1 
		AND COD_LOJA IN (`1`)
		AND CONVERT(DATE,DATA) >= (`2`)
		AND COD_DEPARTAMENTO IN (2)	
GROUP BY
		A.COD_LOJA
	   ,A.COD_PRODUTO
------------------------------------------------------------------------
--SOMANDO GARRAFAS
------------------------------------------------------------------------
SELECT 
		COD_PRODUTO,SUM(QTD_VENDA) AS QTD_VENDA
INTO #TEMP_VENDA_ITENS
FROM #TMP_VENDAS VENDAS

WHERE 1=1 
GROUP BY 
		COD_PRODUTO
------------------------------------------------------------------------
--TRANSF--
------------------------------------------------------------------------	
SELECT 		
		 AJUSTE.COD_PRODUTO
		,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_TRANSF
INTO  #TAB_TRANSF
FROM [192.168.0.6].[ZEUS_RTG].[DBO].TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK)
WHERE 1=1
		AND COD_AJUSTE IN (269)
		AND COD_LOJA IN (29)
		AND CONVERT(DATE,DTA_AJUSTE) >= (`2`)
GROUP BY 
		AJUSTE.COD_PRODUTO			
------------------------------------------------------------------------
--JUNTANDO VENDAS E TRANSFERENCIAS
------------------------------------------------------------------------		
SELECT
		 VENDAS.COD_PRODUTO
		,SUM(QTD_VENDA) AS QTD_VENDA
		,ISNULL(SUM(QTD_TRANSF),0) AS QTD_TRANSF 
INTO #TMP_VENDA_X_TRANSF
FROM #TEMP_VENDA_ITENS VENDAS 
	LEFT JOIN #TAB_TRANSF TRANSF 
			ON TRANSF.COD_PRODUTO = VENDAS.COD_PRODUTO
WHERE 1=1 
		AND QTD_VENDA > 0
GROUP BY 
		VENDAS.COD_PRODUTO
------------------------------------------------------------------------
--DADOS PARA EXCEL INCORRETOS
------------------------------------------------------------------------
SELECT 
		A.COD_PRODUTO Plu
		,Descricao
		,NO_DEPARTAMENTO Departamento
		,QTD_VENDA [Qtde Venda]
		,QTD_TRANSF [Qtde Transf]
		,QTD_VENDA-QTD_TRANSF Dif
FROM #TMP_VENDA_X_TRANSF A 
	LEFT JOIN  BI.DBO.BI_CAD_PRODUTO C 
		ON C.COD_PRODUTO = A.COD_PRODUTO 
 WHERE 1=1
		AND QTD_VENDA-QTD_TRANSF <0
ORDER BY 
		 QTD_VENDA-QTD_TRANSF ASC	 

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja,SQLDateTime@dataIni}];
	r
];
r5\[LeftArrow]getEXCELINCORRETOS[$lojas,$dtIni]


$fileName3="Transferencia maior que a venda Eataly Restaurante - Vinhos em "<>$now2<>".xlsx";
Export["Transferencia maior que a venda Eataly Restaurante - Vinhos em "<>$now2<>".xlsx",r5["DataAll"]];	


marcheMail[
  		"[GE] Pend\[EHat]ncias Transfer\[EHat]ncias Eataly Restaurante(33) - Adega " <>$now 
  		,"Fluxo de vendas de produtos Eataly(33) Restaurante no estoque do Eataly(29):\n
1 - (Loja) - Lan\[CCedilla]ar as quantidades de acordo com a coluna ''A Transf'' na Intranet>Transf Para Estoque Restaurante;
2 - (Loja) - Aprova\[CCedilla]\[ATilde]o da transfer\[EHat]ncia"
(*3.1 - (Loja) - Atrav\[EAcute]s da coluna ''\[AAcute] emitir'', encontrada na planilha ''Pendencia de emiss\[ATilde]o Eataly para Eataly Restaurante'', (Usar sempre o ultimo relat\[OAcute]rio) emitir a nota com os produtos listados no CFOP 192-VENDA DE MERCADORIA (LOJA PARA O RESTAURANTE);
3.2 - (Loja) - Produtos n\[ATilde]o encontrados no momento da emiss\[ATilde]o ou com custo zero, direcionar para o comprador;
4 - (Loja) - Passar a nota para o recebimento, onde dever\[AAcute] ser encaminhada a matriz junto as demais notas;
5 - (Fiscal) - Validar o relat\[OAcute]rio ''Emiss\[ATilde]o de nota com quantidade maior que a transfer\[EHat]ncia'', ele informa inconformidades entre a emiss\[ATilde]o de nota e o lan\[CCedilla]amento da transfer\[EHat]ncia;
5.1 - (Fiscal) - Caso exista diferen\[CCedilla]a entre a quantidade emitida verificar as ultimas notas emitida atrav\[EAcute]s do relat\[OAcute]rio ''NFE Emitida nos \[UAcute]ltimos 3 dias'' e cancelar a NF correspondente na Nortia Nfe e no Zeus."*)
	 ,$mailsGerencia
	 ,{$fileName,$fileName2,$fileName3}	  
]


(*
marcheMail[
  		"Pend\[EHat]ncias Transfer\[EHat]ncias Eataly Restaurante(33) para Eataly(29) - Adega " <>$now 
  		,"Fluxo de vendas de produtos Eataly(33) Restaurante no estoque do Eataly(29):\n
1 - (Loja) - Lan\[CCedilla]ar as quantidades de acordo com a coluna ''A Transf'' na Intranet>Transf Para Estoque Restaurante;
2 - (Loja) - Aprova\[CCedilla]\[ATilde]o da transfer\[EHat]ncia"
(*3.1 - (Loja) - Atrav\[EAcute]s da coluna ''\[AAcute] emitir'', encontrada na planilha ''Pendencia de emiss\[ATilde]o Eataly para Eataly Restaurante'', (Usar sempre o ultimo relat\[OAcute]rio) emitir a nota com os produtos listados no CFOP 192-VENDA DE MERCADORIA (LOJA PARA O RESTAURANTE);
3.2 - (Loja) - Produtos n\[ATilde]o encontrados no momento da emiss\[ATilde]o ou com custo zero, direcionar para o comprador;
4 - (Loja) - Passar a nota para o recebimento, onde dever\[AAcute] ser encaminhada a matriz junto as demais notas;
5 - (Fiscal) - Validar o relat\[OAcute]rio ''Emiss\[ATilde]o de nota com quantidade maior que a transfer\[EHat]ncia'', ele informa inconformidades entre a emiss\[ATilde]o de nota e o lan\[CCedilla]amento da transfer\[EHat]ncia;
5.1 - (Fiscal) - Caso exista diferen\[CCedilla]a entre a quantidade emitida verificar as ultimas notas emitida atrav\[EAcute]s do relat\[OAcute]rio ''NFE Emitida nos \[UAcute]ltimos 3 dias'' e cancelar a NF correspondente na Nortia Nfe e no Zeus."*)
	 ,"diego.miller@marche.com.br"
	 ,{$fileName,$fileName2,$fileName3}	  
]
*)
