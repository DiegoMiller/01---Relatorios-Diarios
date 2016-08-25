(* ::Package:: *)

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


$lojas={1,2,3,6,7,9,13,17,18,20,21,22,23,24,25,27,29,30,31};
$deParaLoja=maGetDeParaLojas[];
$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];


1


SystemOpen@$BaseDirectory


getDPD[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 
		SET NOCOUNT ON;
		SELECT
				 COD_LOJA
				,COD_PRODUTO
				,CAST(SUM(QTDE_PRODUTO) AS DOUBLE PRECISION) AS VENDA
		INTO #TAB_TEMP_VENDA
		FROM BI.DBO.BI_VENDA_PRODUTO 
		WHERE 1=1
				AND COD_LOJA IN (`1`)
				AND DATA >=  CONVERT(DATE,GETDATE()-365)
		GROUP BY
				COD_LOJA
				,COD_PRODUTO
		---------------------------------------------------------
		SELECT
				 COD_LOJA
				,COD_PRODUTO
				,SUM(QTD_AJUSTE) AS QTD_QUEBRA
		INTO #TAB_TEMP_QUEBRA_365_DIAS
		FROM [192.168.0.6].ZEUS_RTG.DBO.TAB_AJUSTE_ESTOQUE WITH (NOLOCK)
		WHERE 1=1
				AND COD_LOJA IN (`1`)
				AND DTA_AJUSTE >=  CONVERT(DATE,GETDATE()-365)
				AND COD_AJUSTE IN (155,120,123,124,154,122,51,121)
		GROUP BY 	
				 COD_LOJA
				,COD_PRODUTO						
		---------------------------------------------------------	
		SELECT
				 COD_LOJA
				,COD_PRODUTO
				,CAST(QTD_ESTOQUE AS DOUBLE PRECISION) AS QTD_ESTOQUE
		INTO #TAB_TEMP_ESTOQUE_ATUAL
		FROM DW.DBO.ESTOQUE WITH (NOLOCK)
		WHERE 1=1
				AND COD_LOJA IN (`1`)
				AND DATA = CONVERT(DATE,GETDATE()-1)
				AND QTD_ESTOQUE >= 0
		---------------------------------------------------------
		SELECT
				 COD_LOJA
				,DTA_ENTRADA
				,ENTRADA.COD_PRODUTO
				,NO_DEPARTAMENTO
				,DESCRICAO
				,ENTRADA.COD_FORNECEDOR
				,DES_FORNECEDOR
				,(QTD_ENTRADA*QTD_EMBALAGEM) AS ENTRADA
				,NO_Comprador
		INTO #TAB_TEMP_ENTRADAS_HOJE
		FROM [192.168.0.6].[ZEUS_RTG].[DBO].[VW_MARCHE_ENTRADAS] AS ENTRADA  WITH (NOLOCK)
		LEFT JOIN 
		BI.DBO.BI_CAD_PRODUTO AS CP ON ENTRADA.COD_PRODUTO = CP.COD_PRODUTO
		LEFT JOIN 
		BI.DBO.COMPRAS_CAD_COMPRADOR AS COMPRADOR ON CP.COD_USUARIO = COMPRADOR.COD_USUARIO
		WHERE 1=1
				AND COD_LOJA IN (`1`)
				AND DTA_ENTRADA = CONVERT(DATE,GETDATE())
				AND CP.COD_SECAO IN (7,8,9,11,13,17,22,23,26,27,34,35,41,42,44,45,46,47,48,50,52,53,54,55,60,61,62,64,65,71,72,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97)
				AND CP.COD_DEPARTAMENTO IN (6,1,4,8,12,21,3,9,2,13,11,10)
		---------------------------------------------------------
		SELECT DISTINCT COD_LOJA, COD_PRODUTO, COD_FORNECEDOR, CAST(QTD_MINIMA_COMPRA*QTD_EMBALAGEM_COMPRA AS DOUBLE PRECISION) AS QTD_MINIMA_COMPRA INTO #TEMP_QTD_MINIMA FROM [192.168.0.6].ZEUS_RTG.DBO.TAB_PRODUTO_FORNECEDOR 
		---------------------------------------------------------				
		SELECT TOP 10
				 NO_LOJA AS Loja
				,HOJE.COD_PRODUTO*1 AS Plu
				,DESCRICAO AS Descricao
				,NO_DEPARTAMENTO AS Departamento
				,QTD_ESTOQUE AS [Qtd Est Ant]
				,ENTRADA AS [Qtd Ult Entrada]
				,(QTD_MINIMA_COMPRA) AS [Qtd Min Compra]
				,CONVERT(DECIMAL(18,0),(ISNULL(VENDA,0))) AS [Qtd Venda]
				,CONVERT(DECIMAL(18,0),((QTD_ESTOQUE+ENTRADA))/((VENDA/365))) AS DPD 
				,ISNULL(QTD_QUEBRA,0)*-1 AS [Qtd Quebra]
				,CONVERT(VARCHAR,DTA_ENTRADA,103) AS [Dta Entrada]
				,DES_FORNECEDOR AS Forn
				,NO_COMPRADOR AS Comprador
		FROM #TAB_TEMP_ENTRADAS_HOJE AS HOJE 
		LEFT JOIN #TAB_TEMP_ESTOQUE_ATUAL AS ESTOQUE  ON HOJE.COD_LOJA = ESTOQUE.COD_LOJA AND HOJE.COD_PRODUTO = ESTOQUE.COD_PRODUTO 
		LEFT JOIN #TAB_TEMP_VENDA AS VENDA ON HOJE.COD_LOJA = VENDA.COD_LOJA AND HOJE.COD_PRODUTO = VENDA.COD_PRODUTO
		LEFT JOIN [BI].[DBO].[BI_CAD_LOJA2] AS LJ ON LJ.COD_LOJA = HOJE.COD_LOJA
		LEFT JOIN #TAB_TEMP_QUEBRA_365_DIAS AS QUEBRA ON HOJE.COD_LOJA = QUEBRA.COD_LOJA AND HOJE.COD_PRODUTO = QUEBRA.COD_PRODUTO 
		LEFT JOIN #TEMP_QTD_MINIMA AS QTDMINIMA ON QTDMINIMA.COD_LOJA = HOJE.COD_LOJA AND QTDMINIMA.COD_PRODUTO = HOJE.COD_PRODUTO AND QTDMINIMA.COD_FORNECEDOR = HOJE.COD_FORNECEDOR
		
		WHERE 1=1
				AND VENDA >0
				--AND QTD_ESTOQUE
				AND HOJE.COD_LOJA IN (`1`)
		ORDER BY 
				((QTD_ESTOQUE+ENTRADA))/((VENDA/365)) DESC 
		---------------------------------------------------------		
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r["CodLoja"]=codLoja;
	r["NoLoja"]=codLoja/.$deParaLoja;
	r["FileName"]=ToString@Row@{"DPD ENTRADAS ",r["NoLoja"]," ",$now2,".png"};
	r
]
r[99]=getDPD[$lojas];


i=0;Dynamic@i
Scan[(i++;r[#]=getDPD[#])&,$lojas]


gridDPD[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
			#>30,Red
			,#==30,Orange
			,True,Darker@Green

	]&;

	stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="";

	grid=maReportGrid[r,"ColumnFormat"->{{"DPD"}-> stl,{"Qtd Est Ant","Qtd Quebra","Qtd Venda","Qtd Ult Entrada","Qtd Min Compra"}-> "N"},"TotalRow"->False];
	title=Style[Row@{"DPD de entradas sobre a venda das ultimas 52 semanas em ", $now},maTitleFormatOptions];
	maReportFrame[title,grid]
]
gridDPD[r[99]]


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};


$fileName="TOP 10 DPD ENTRADAS REDE "<>$now2<>".png";
Export["TOP 10 DPD ENTRADAS REDE "<>$now2<>".png",gridDPD[r[99]]];


createFile[codLoja_Integer]:=Module[{},
	Export[r[codLoja]["FileName"],gridDPD[r[codLoja]]]
]


i=0;Dynamic@i
Scan[(i++;createFile[#])&,$lojas[[]]]


marcheMail[
		"[GE] Top 10 DPD entradas em "<>$now
		,"Top 10 entradas maiores que a venda das ultimas 52 semanas"
		,$mailsGerencia
		,{$fileName,r[1]["FileName"],r[2]["FileName"],r[3]["FileName"],r[6]["FileName"],r[7]["FileName"],r[9]["FileName"],r[13]["FileName"],r[17]["FileName"],r[18]["FileName"],r[20]["FileName"],r[21]["FileName"],r[22]["FileName"],r[23]["FileName"],r[24]["FileName"],r[25]["FileName"],r[27]["FileName"],r[30]["FileName"],r[31]["FileName"]}
]
