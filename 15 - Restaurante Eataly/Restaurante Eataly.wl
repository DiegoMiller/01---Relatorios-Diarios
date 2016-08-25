(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$now3=DateString[DatePlus[-1],{"Day","-","Month","-","Year"}];
$lojas={29};
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["Marche.png"];


getALIMENTUM[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 
		
		SET NOCOUNT ON;	
			
		SELECT 
				COD_LOJA
				,AJUSTE.COD_PRODUTO
				,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_AJUSTE
				,SUM((QTD_AJUSTE*VAL_CUSTO_REP)*-1) AS CUSTO_TRANSF
				INTO #TAB_AJUSTE_TEMP
		FROM TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK) 
				WHERE 1=1
				AND COD_AJUSTE = 269
				AND COD_LOJA in (`1`)
		GROUP BY 
				COD_LOJA
				,AJUSTE.COD_PRODUTO
			
		---------------------------------------------------		
		--SAIDAS--
		---------------------------------------------------	

		SELECT 
				COD_PRODUTO
				,SUM(CAST((QTD_EMBALAGEM*QTD_ENTRADA) AS DOUBLE PRECISION)) AS QTD_EMITIDA
		INTO #TAB_EMITIDA
		FROM [ZEUS_RTG].[DBO].[VW_NF_SAIDA]
		WHERE 1=1
				AND COD_LOJA in (`1`)
				AND COD_CLIENTE = 1960502441		
				AND COD_FISCAL = 192	
		GROUP BY 
				COD_PRODUTO

		---------------------------------------------------		
		--BUSCANDO CUSTOS--
		---------------------------------------------------		

		SELECT 
				COD_LOJA
				,COD_PRODUTO
				,CUSTO
				,CUSTO_FIXO
				,CUSTO_ENTRADA
				,CUSTO_AJUSTADO
				,CUSTO_CMV
				,CUSTO_MEDIO
				,CUSTO_ULTIMA_ZEUS
				,CUSTO_TABELA
		INTO #TMP_CUSTO 
		FROM [192.168.0.13].DW.DBO.PRODUTO_CUSTOS 
		WHERE 1=1
			   AND COD_PRODUTO IN (SELECT DISTINCT COD_PRODUTO FROM #TAB_AJUSTE_TEMP WHERE COD_LOJA IN (`1`))
			   AND COD_LOJA in  (`1`)

	 
		---------------------------------------------------		
		--JUNTANDO DADOS--
		---------------------------------------------------		

		SELECT 
				AJUSTE.COD_PRODUTO*1 AS PLU
				,DESCRICAO
				,NO_DEPARTAMENTO
				,ISNULL(UNIDADE_VENDA,'UN') AS UNIDADE_VENDA
				,QTD_AJUSTE
				,ISNULL(QTD_EMITIDA,0)  AS QTD_EMITIDA
				,ISNULL(CAST((QTD_AJUSTE-QTD_EMITIDA) AS DOUBLE PRECISION),QTD_AJUSTE) AS [\[CapitalAGrave] EMITIR]
				,CAST(CUSTO AS DOUBLE PRECISION) CUSTO
				,CAST(CUSTO_FIXO AS DOUBLE PRECISION) CUSTO_FIXO
				,CAST(CUSTO_ENTRADA AS DOUBLE PRECISION) CUSTO_ENTRADA
				,CAST(CUSTO_AJUSTADO AS DOUBLE PRECISION) CUSTO_AJUSTADO
				,CAST(CUSTO_CMV AS DOUBLE PRECISION) CUSTO_CMV
				,CAST(CUSTO_MEDIO AS DOUBLE PRECISION) CUSTO_MEDIO
				,CAST(CUSTO_ULTIMA_ZEUS AS DOUBLE PRECISION) CUSTO_ULTIMA_ZEUS
				,CAST(CUSTO_TABELA AS DOUBLE PRECISION) CUSTO_TABELA
		FROM #TAB_AJUSTE_TEMP AS AJUSTE 
		LEFT JOIN #TAB_EMITIDA ON AJUSTE.COD_PRODUTO = #TAB_EMITIDA.COD_PRODUTO 
		LEFT JOIN #TMP_CUSTO CUSTO ON CUSTO.COD_PRODUTO = AJUSTE.COD_PRODUTO
		LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO AS CAD ON AJUSTE.COD_PRODUTO = CAD.COD_PRODUTO
		WHERE 1=1
				AND ISNULL(CAST((QTD_AJUSTE-QTD_EMITIDA) AS DOUBLE PRECISION),QTD_AJUSTE) > 0.101
				AND AJUSTE.COD_LOJA IN (`1`)
				--AND CUSTO IS NULL
		
		ORDER BY 
				ISNULL(CAST((QTD_AJUSTE-QTD_EMITIDA) AS DOUBLE PRECISION),QTD_AJUSTE)

		DROP TABLE #TAB_AJUSTE_TEMP
		DROP TABLE #TAB_EMITIDA
		DROP TABLE #TMP_CUSTO

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r
];
r\[LeftArrow]getALIMENTUM[$lojas]


getTOP23[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 
		SET NOCOUNT ON;	
			
		---------------------------------------------------		
		--AJUSTES--
		---------------------------------------------------	
	
		SELECT 
				COD_LOJA
				,AJUSTE.COD_PRODUTO
				,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_AJUSTE
				,SUM((QTD_AJUSTE*VAL_CUSTO_REP)*-1) AS CUSTO
				INTO #TAB_AJUSTE_TEMP
		FROM TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK) 
				WHERE 1=1
				AND COD_AJUSTE = 269
				AND COD_LOJA = 29
		GROUP BY 
				COD_LOJA
				,AJUSTE.COD_PRODUTO
			
		---------------------------------------------------		
		--SAIDAS--
		---------------------------------------------------	

		SELECT 
				COD_PRODUTO
				,SUM(CAST((QTD_EMBALAGEM*QTD_ENTRADA) AS DOUBLE PRECISION)) AS QTD_EMITIDA
		INTO #TAB_EMITIDA
		FROM [ZEUS_RTG].[DBO].[VW_NF_SAIDA]
		WHERE 1=1
				AND COD_LOJA = 29
				AND COD_CLIENTE = 1960502441		
				AND COD_FISCAL = 192	
		GROUP BY 
				COD_PRODUTO

		---------------------------------------------------		
		--JUNTANDO DADOS--
		---------------------------------------------------	

		SELECT TOP 23
				AJUSTE.COD_PRODUTO*1 as Plu
				,Descricao
				,NO_DEPARTAMENTO as Departamento
				,QTD_AJUSTE as [Qtde Transf]
				,ISNULL(QTD_EMITIDA,0) AS [Qtde Emitida]
				,ISNULL(CAST((QTD_AJUSTE-QTD_EMITIDA) AS DOUBLE PRECISION),QTD_AJUSTE) AS [A Emitir]
		FROM #TAB_AJUSTE_TEMP AS AJUSTE 
		LEFT JOIN #TAB_EMITIDA ON AJUSTE.COD_PRODUTO = #TAB_EMITIDA.COD_PRODUTO
		LEFT JOIN TAB_PRODUTO_LOJA AS REP ON REP.COD_LOJA = AJUSTE.COD_LOJA AND REP.COD_PRODUTO = AJUSTE.COD_PRODUTO 
		LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO AS CAD ON AJUSTE.COD_PRODUTO = CAD.COD_PRODUTO
		WHERE 1=1
				AND ISNULL(CAST((QTD_AJUSTE-QTD_EMITIDA) AS DOUBLE PRECISION),QTD_AJUSTE) > 0.101
				AND AJUSTE.COD_LOJA IN (`1`)
		ORDER BY 
				ISNULL(CAST((QTD_AJUSTE-QTD_EMITIDA) AS DOUBLE PRECISION),QTD_AJUSTE) DESC

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r
];
r2\[LeftArrow]getTOP23[$lojas]


grid8[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
	 #<=0,Darker@Green
	,True,Red
	]&;

    stl[val_]:=Style[maF["N"][val],Bold,color2@val];
	stl[Null]="0"; 

	grid=maReportGrid[r,"ColumnFormat"->{{"A Emitir"}-> stl,{"Qtde Transf","Qtde Emitida"}-> "N"},"TotalRow"->False];
	title=Style[Row@{"Top Itens"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
grid8[r2];


getTOTAL[codLoja_]:=Module[{sql,s,conn=marcheConn[],tab},
	sql=" 

		SET NOCOUNT ON;	
		
	    ---------------------------------------------------		
		--AJUSTES--
		---------------------------------------------------	
	
		SELECT 
				COD_LOJA
				,AJUSTE.COD_PRODUTO
				,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_AJUSTE
				,SUM((QTD_AJUSTE*VAL_CUSTO_REP)*-1) AS CUSTO
				INTO #TAB_AJUSTE_TEMP
		FROM TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK) 
				WHERE 1=1
				AND COD_AJUSTE = 269
				AND COD_LOJA = 29
		GROUP BY 
				COD_LOJA
				,AJUSTE.COD_PRODUTO
			
		---------------------------------------------------		
		--SAIDAS--
		---------------------------------------------------	

		SELECT 
				COD_PRODUTO
				,SUM(CAST((QTD_EMBALAGEM*QTD_ENTRADA) AS DOUBLE PRECISION)) AS QTD_EMITIDA
		INTO #TAB_EMITIDA
		FROM [ZEUS_RTG].[DBO].[VW_NF_SAIDA]
		WHERE 1=1
				AND COD_LOJA = 29
				AND COD_CLIENTE = 1960502441		
				AND COD_FISCAL = 192	
		GROUP BY 
				COD_PRODUTO

		---------------------------------------------------		
		--JUNTANDO DADOS--
		---------------------------------------------------	
			
	    SELECT 
				NO_DEPARTAMENTO
				,COUNT(AJUSTE.COD_PRODUTO) as Qtde
		INTO #TAB_PENDENCIAS_TOTAL_TEMP
		FROM #TAB_AJUSTE_TEMP AS AJUSTE 
		LEFT JOIN #TAB_EMITIDA ON AJUSTE.COD_PRODUTO = #TAB_EMITIDA.COD_PRODUTO 
		LEFT JOIN TAB_PRODUTO_LOJA AS REP ON REP.COD_LOJA = AJUSTE.COD_LOJA AND REP.COD_PRODUTO = AJUSTE.COD_PRODUTO
		LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO AS CAD ON AJUSTE.COD_PRODUTO = CAD.COD_PRODUTO 
		WHERE 1=1
				AND ISNULL(CAST((QTD_AJUSTE-QTD_EMITIDA) AS DOUBLE PRECISION),QTD_AJUSTE) > 0.101
				AND AJUSTE.COD_LOJA IN (`1`)
		GROUP BY 
				NO_DEPARTAMENTO
		
		SELECT DISTINCT NO_DEPARTAMENTO INTO #TEMP_RESUMO FROM [192.168.0.13].BI.DBO.BI_CAD_PRODUTO
		
			
		SELECT A.NO_DEPARTAMENTO AS Departamento, ISNULL(Qtde,0) as Qtde  FROM  #TEMP_RESUMO AS A LEFT JOIN #TAB_PENDENCIAS_TOTAL_TEMP AS B ON A.NO_DEPARTAMENTO = B.NO_DEPARTAMENTO
		UNION		
		SELECT 'TOTAL' as TOTAL, SUM(QTDE)as QTDE FROM  #TAB_PENDENCIAS_TOTAL_TEMP
		
 ";
		s\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
		s
];
s\[LeftArrow]getTOTAL[$lojas]


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

rep=marcheTemplate[grid=Row@{"Pend\[EHat]ncias de emiss\[ATilde]o de NF da 29-Eataly para 33-Eataly Restaurante em ",$now},createReport[s],1200, $logo];


getALIMENTUM[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 
		SET NOCOUNT ON;	
	
		SELECT 
				COD_LOJA
				,AJUSTE.COD_PRODUTO
				,DESCRICAO
				,NO_DEPARTAMENTO
				,UNIDADE_VENDA
				,SUM(CAST(QTD_AJUSTE AS DOUBLE PRECISION))*-1 AS QTD_AJUSTE
				,SUM((QTD_AJUSTE*VAL_CUSTO_REP)*-1) AS CUSTO
				INTO #TAB_AJUSTE_TEMP
		FROM TAB_AJUSTE_ESTOQUE AS AJUSTE WITH (NOLOCK) LEFT JOIN [192.168.0.13].BI.DBO.BI_CAD_PRODUTO AS CAD ON AJUSTE.COD_PRODUTO = CAD.COD_PRODUTO 
		WHERE 1=1
				AND COD_AJUSTE IN (269)
				AND COD_LOJA IN (`1`)
		GROUP BY 
				COD_LOJA
				,AJUSTE.COD_PRODUTO
				,DESCRICAO
				,NO_DEPARTAMENTO
				,UNIDADE_VENDA
		-----------------------------------------------------

		SELECT 
				COD_PRODUTO
				,SUM(CAST((QTD_EMBALAGEM*QTD_ENTRADA) AS DOUBLE PRECISION)) AS QTD_EMITIDA
		INTO #TAB_EMITIDA
		FROM [ZEUS_RTG].[DBO].[VW_NF_SAIDA]
		WHERE 1=1
				AND COD_LOJA IN (`1`)
				AND COD_CLIENTE = 1960502441		
				AND COD_FISCAL = 192	
		GROUP BY 
				COD_PRODUTO

		--BUSCANDO CUSTOS DE OUTRAS LOJAS--
		SELECT DISTINCT COD_PRODUTO, MAX(VAL_CUSTO_REP) AS VAL_CUSTO_REP INTO #TEMP_CUSTO FROM TAB_PRODUTO_LOJA GROUP BY COD_PRODUTO 
	
			

		SELECT 
				AJUSTE.COD_PRODUTO*1 as PLU
				,DESCRICAO
				,NO_DEPARTAMENTO
				,ISNULL(UNIDADE_VENDA,'UN') as UNIDADE_VENDA
				,QTD_AJUSTE
				,ISNULL(QTD_EMITIDA,0)  AS QTD_EMITIDA
				,ISNULL(CAST((QTD_AJUSTE-QTD_EMITIDA) AS DOUBLE PRECISION),QTD_AJUSTE) AS [DIFERENCAS]
				,CASE REP.VAL_CUSTO_REP WHEN 0 THEN C.VAL_CUSTO_REP ELSE REP.VAL_CUSTO_REP END AS VAL_CUSTO_REP
				,VAL_CUSTO_TABELA
		INTO #TEMP_RESUMO
		FROM #TAB_AJUSTE_TEMP AS AJUSTE LEFT JOIN #TAB_EMITIDA ON AJUSTE.COD_PRODUTO = #TAB_EMITIDA.COD_PRODUTO LEFT JOIN TAB_PRODUTO_LOJA AS REP ON REP.COD_LOJA = AJUSTE.COD_LOJA AND REP.COD_PRODUTO = AJUSTE.COD_PRODUTO LEFT JOIN #TEMP_CUSTO AS C on c.cod_produto = 	AJUSTE.cod_produto
		WHERE 1=1
				AND ISNULL(CAST((QTD_AJUSTE-QTD_EMITIDA) AS DOUBLE PRECISION),QTD_AJUSTE) < -0.101
			
		ORDER BY 
				ISNULL(CAST((QTD_AJUSTE-QTD_EMITIDA) AS DOUBLE PRECISION),QTD_AJUSTE) DESC		
							
		SELECT 
				PLU
				,DESCRICAO
				,NO_DEPARTAMENTO
				,UNIDADE_VENDA
				,QTD_AJUSTE
				,QTD_EMITIDA
				,[DIFERENCAS]
				--,CASE VAL_CUSTO_REP WHEN 0 THEN VAL_CUSTO_TABELA ELSE VAL_CUSTO_REP END AS VAL_CUSTO_REP 
		FROM #TEMP_RESUMO 	

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r
];
r3\[LeftArrow]getALIMENTUM[$lojas]


getNFONTEM[codLoja_]:=Module[{sql,r,conn=marcheConn[],tab},
	sql=" 
		SET NOCOUNT ON;	
		
		SELECT 
			COD_CLIENTE
			,COD_FISCAL
			,DES_CODIGO_FISCAL
			,NUM_NF_CLI
			,NUM_SERIE_NF
			,CONVERT(DATE,DTA_EMISSAO) DTA_EMISSAO
			,DES_ESPECIE
			,NUM_ITEM
			,COD_PRODUTO
			,GERA_ESTOQUE
			,QTD_EMBALAGEM
			,QTD_ENTRADA
			,DES_PRODUTO
			,VAL_TOTAL_NF
		FROM [ZEUS_RTG].[DBO].[VW_NF_SAIDA]
		WHERE 1=1
				AND COD_LOJA IN (`1`)
				AND COD_CLIENTE = 1960502441		
				AND COD_FISCAL = 192	
				AND DTA_EMISSAO >= CONVERT(DATE,GETDATE()-3)
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r
];
r4\[LeftArrow]getNFONTEM[$lojas]


$fileName1="Pend\[EHat]ncias de emiss\[ATilde]o de NF da 29-Eataly para 33-Eataly Restaurante em "<>$now2<>".png";
Export["Pend\[EHat]ncias de emiss\[ATilde]o de NF da 29-Eataly para 33-Eataly Restaurante em "<>$now2<>".png",rep];


$fileName="Pendencia de emiss\[ATilde]o Eataly para Eataly Restaurante em "<>$now2<>".xlsx";
Export["Pendencia de emiss\[ATilde]o Eataly para Eataly Restaurante em "<>$now2<>".xlsx",r["DataAll"]];		


$fileName2="Emissao de nota com quantidade maior que a transferencia "<>$now2<>".xlsx";
Export["Emissao de nota com quantidade maior que a transferencia "<>$now2<>".xlsx",r3["DataAll"]];		


$fileName3="NFE Emitida nos ultimos 3 dias.xlsx";
Export["NFE Emitida nos ultimos 3 dias.xlsx",r4["DataAll"]];		


marcheMail[
  		"[GE] Pend\[EHat]ncias de emiss\[ATilde]o de NF Eataly Loja PARA Eataly Restaurante em " <>$now 
  		,"Fluxo de movimenta\[CCedilla]\[ATilde]o de produtos do Eataly para Eataly Restaurante\n
1 - (Loja) - Lan\[CCedilla]amento da Transfer\[EHat]ncia na Intranet atrav\[EAcute]s da op\[CCedilla]\[ATilde]o TRANSF PARA ESTOQUE RESTAURANTE;
2 - (Loja) - Aprova\[CCedilla]\[ATilde]o da transfer\[EHat]ncia;
3.1 - (Fiscal) - Atrav\[EAcute]s da coluna ''\[AAcute] emitir'', encontrada na planilha ''EXCLUSIVO ESM - VENDA HORTUS PARA ALIMENTUM CFOP 192'', (Usar sempre o ultimo relat\[OAcute]rio) emitir a nota com os produtos listados no CFOP 192-VENDA DE MERCADORIA (LOJA PARA O RESTAURANTE), Usar o valor VLRCMV_ENTRADA como principal, caso esteja zerado ir para coluna seguinte , assim sucessivamente (VLR_CUSTO_MEDIO, VAL_CUSTO_TABELA, VAL_CUSTO_REP , VLR_VENDA);
3.2 - (Fiscal) - Produtos n\[ATilde]o encontrados no momento da emiss\[ATilde]o ou com custo zero, direcionar para o comprador;
4 - (Fiscal) - Passar a nota para o recebimento, onde dever\[AAcute] ser encaminhada a matriz junto as demais notas;
5 - (Fiscal) - Validar o relat\[OAcute]rio ''Emiss\[ATilde]o de nota com quantidade maior que a transfer\[EHat]ncia'', ele informa inconformidades entre a emiss\[ATilde]o de nota e o lan\[CCedilla]amento da transfer\[EHat]ncia;
5.1 - (Fiscal) - Caso exista diferen\[CCedilla]a entre a quantidade emitida verificar as ultimas notas emitida atrav\[EAcute]s do relat\[OAcute]rio ''NFE Emitida nos \[UAcute]ltimos 3 dias'' e cancelar a NF correspondente na Nortia Nfe e no Zeus."
	 ,$mailsGerencia
	 ,{$fileName1,$fileName,$fileName2,$fileName3}	  
]
