(* ::Package:: *)

Needs["Murta`"]
Needs["MAFormat`"]
Needs["MarcheDiego`"]


$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$lojas={1,2,3,5,6,7,9,12,13,17,18,20,21,22,23,24,25,27,28,29,30,31,33,9999};
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
$logo=Import["Marche.png"];


getALIMENTUM[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 
	
			SET NOCOUNT ON;	
		
			---------------------------------------------------		
			--BUSCANDO NFS DE VENDA--
			---------------------------------------------------	

			SELECT 
					CODFIL COD_LOJA
					,NUMNOTA
					,SERIE
					,CONVERT(DATE,DTNOTA) DTNOTA
					,SUBSTRING (REPLICATE ( '0' ,(10 - LEN(CODIGO_ITEM)) ) + CAST(CODIGO_ITEM AS VARCHAR(10)) ,1,9)*1 AS COD_PRODUTO 
					,DESCPROD
					,CAST(QTDETRIB AS DOUBLE PRECISION) QTDETRIB
					,CAST(VLUNIDTRIBUTAVEL AS DOUBLE PRECISION) VLUNIDTRIBUTAVEL
					,CAST(VLPRODUTO AS DOUBLE PRECISION) VLPRODUTO_TOTAL
			INTO  #TMP_ULTIMAS_NF
			FROM [192.168.0.6].DBNFE.DBO.ST_VW_ITENS_NFSAIDA WITH (NOLOCK) 
			WHERE 1=1
				AND CONVERT(DATE,DTNOTA) >= CONVERT(DATE,GETDATE()-3)
				AND CFOP IN (5552,5552,5152,5152,5151,5151,5551,6551,6102,5102,5102,5101,5102,5102,6102,5102,5102)

			---------------------------------------------------		
			--BUSCANDO VLR DE VENDA--
			---------------------------------------------------	

			SELECT COD_LOJA	,COD_PRODUTO ,CAST(VLR_VENDA AS DOUBLE PRECISION) VLR_VENDA INTO #TMP_VAL_VENDA FROM [BI].[DBO].[VW_PRECOS_VENDA_ATIVOS] WHERE 1=1 AND COD_PRODUTO IN (SELECT COD_PRODUTO FROM #TMP_ULTIMAS_NF) AND VLR_VENDA <> 0

			---------------------------------------------------		
			--BUSCANDO CUSTO DE VENDA DE OUTRAS LOJAS--
			---------------------------------------------------	

			DECLARE @RETORNO AS TABLE 
			( 
				COD_PRODUTO INT
				,VLR_VENDA DOUBLE PRECISION 	
			)

			DECLARE DB_CURSOR CURSOR FOR  
			(SELECT DISTINCT COD_PRODUTO FROM #TMP_ULTIMAS_NF) 

			DECLARE @COD_PRODUTO INT

			OPEN DB_CURSOR   
			FETCH NEXT FROM DB_CURSOR INTO @COD_PRODUTO

			WHILE @@FETCH_STATUS = 0   
			BEGIN   

			INSERT INTO @RETORNO (COD_PRODUTO,VLR_VENDA)
			
						SELECT TOP 1 COD_PRODUTO, VLR_VENDA FROM  #TMP_VAL_VENDA 
						WHERE 1=1
						AND VLR_VENDA <> 0
						AND COD_PRODUTO = @COD_PRODUTO

				   FETCH NEXT FROM DB_CURSOR INTO @COD_PRODUTO   
			END   

			CLOSE DB_CURSOR   
			DEALLOCATE DB_CURSOR

			---------------------------------------------------		
			--RELACIONANDO RESULTADO--
			---------------------------------------------------	
	
			SELECT 
					NF.COD_LOJA
					,NUMNOTA
					,SERIE
					,DTNOTA
					,NF.COD_PRODUTO
					,DESCPROD
					,QTDETRIB
					,VLPRODUTO_TOTAL
					,VLUNIDTRIBUTAVEL
					,ISNULL(VLRVENDA.VLR_VENDA,VLRVENDAGERAL.VLR_VENDA) VLRVENDA
					,CASE WHEN ISNULL(VLRVENDA.VLR_VENDA,VLRVENDAGERAL.VLR_VENDA) < VLUNIDTRIBUTAVEL THEN 1 ELSE 0 END VALIDACAO
			INTO #TMP_RELACIONANDO
			FROM #TMP_ULTIMAS_NF NF 
			LEFT JOIN #TMP_VAL_VENDA VLRVENDA 
				 ON VLRVENDA.COD_LOJA = NF.COD_LOJA AND   VLRVENDA.COD_PRODUTO = NF.COD_PRODUTO
			LEFT JOIN @RETORNO VLRVENDAGERAL 
				 ON  VLRVENDAGERAL.COD_PRODUTO = NF.COD_PRODUTO

			---------------------------------------------------		
			--CRIANDO RESUMO--
			---------------------------------------------------	

			SELECT * FROM #TMP_RELACIONANDO WHERE 1=1 
			AND VALIDACAO = 1 
			AND COD_LOJA IN (`1`)

";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r
];
r\[LeftArrow]getALIMENTUM[$lojas]


getTOP23[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 

	SET NOCOUNT ON;	
		
			---------------------------------------------------		
			--BUSCANDO NFS DE VENDA--
			---------------------------------------------------	

			SELECT 
					CODFIL COD_LOJA
					,NUMNOTA
					,SERIE
					,CONVERT(DATE,DTNOTA) DTNOTA
					,SUBSTRING (REPLICATE ( '0' ,(10 - LEN(CODIGO_ITEM)) ) + CAST(CODIGO_ITEM AS VARCHAR(10)) ,1,9)*1 AS COD_PRODUTO 
					,DESCPROD
					,CAST(QTDETRIB AS DOUBLE PRECISION) QTDETRIB
					,CAST(VLUNIDTRIBUTAVEL AS DOUBLE PRECISION) VLUNIDTRIBUTAVEL
					,CAST(VLPRODUTO AS DOUBLE PRECISION) VLPRODUTO_TOTAL
			INTO  #TMP_ULTIMAS_NF
			FROM [192.168.0.6].DBNFE.DBO.ST_VW_ITENS_NFSAIDA WITH (NOLOCK) 
			WHERE 1=1
				AND CONVERT(DATE,DTNOTA) >= CONVERT(DATE,GETDATE()-3)
				AND CFOP IN (5552,5552,5152,5152,5151,5151,5551,6551,6102,5102,5102,5101,5102,5102,6102,5102,5102)

			---------------------------------------------------		
			--BUSCANDO VLR DE VENDA--
			---------------------------------------------------	

			SELECT COD_LOJA	,COD_PRODUTO ,CAST(VLR_VENDA AS DOUBLE PRECISION) VLR_VENDA INTO #TMP_VAL_VENDA FROM [BI].[DBO].[VW_PRECOS_VENDA_ATIVOS] WHERE 1=1 AND COD_PRODUTO IN (SELECT COD_PRODUTO FROM #TMP_ULTIMAS_NF) AND VLR_VENDA <> 0

			---------------------------------------------------		
			--BUSCANDO CUSTO DE VENDA DE OUTRAS LOJAS--
			---------------------------------------------------	

			DECLARE @RETORNO AS TABLE 
			( 
				COD_PRODUTO INT
				,VLR_VENDA DOUBLE PRECISION 	
			)

			DECLARE DB_CURSOR CURSOR FOR  
			(SELECT DISTINCT COD_PRODUTO FROM #TMP_ULTIMAS_NF) 

			DECLARE @COD_PRODUTO INT

			OPEN DB_CURSOR   
			FETCH NEXT FROM DB_CURSOR INTO @COD_PRODUTO

			WHILE @@FETCH_STATUS = 0   
			BEGIN   

			INSERT INTO @RETORNO (COD_PRODUTO,VLR_VENDA)
			
						SELECT TOP 1 COD_PRODUTO, VLR_VENDA FROM  #TMP_VAL_VENDA 
						WHERE 1=1
						AND VLR_VENDA <> 0
						AND COD_PRODUTO = @COD_PRODUTO

				   FETCH NEXT FROM DB_CURSOR INTO @COD_PRODUTO   
			END   

			CLOSE DB_CURSOR   
			DEALLOCATE DB_CURSOR

			---------------------------------------------------		
			--RELACIONANDO RESULTADO--
			---------------------------------------------------	
	
			SELECT 
					NF.COD_LOJA
					,NUMNOTA
					,SERIE
					,DTNOTA
					,NF.COD_PRODUTO
					,DESCPROD
					,QTDETRIB
					,VLPRODUTO_TOTAL
					,VLUNIDTRIBUTAVEL
					,ISNULL(VLRVENDA.VLR_VENDA,VLRVENDAGERAL.VLR_VENDA) VLRVENDA
					,CASE WHEN ISNULL(VLRVENDA.VLR_VENDA,VLRVENDAGERAL.VLR_VENDA) < VLUNIDTRIBUTAVEL THEN 1 ELSE 0 END VALIDACAO
			INTO #TMP_RELACIONANDO
			FROM #TMP_ULTIMAS_NF NF 
			LEFT JOIN #TMP_VAL_VENDA VLRVENDA 
				 ON VLRVENDA.COD_LOJA = NF.COD_LOJA AND   VLRVENDA.COD_PRODUTO = NF.COD_PRODUTO
			LEFT JOIN @RETORNO VLRVENDAGERAL 
				 ON  VLRVENDAGERAL.COD_PRODUTO = NF.COD_PRODUTO

			---------------------------------------------------		
			--CRIANDO RESUMO--
			---------------------------------------------------	

			SELECT TOP 24 NO_LOJA Loja	,convert(varchar,DTNOTA,103) [Dta Nota]	,COD_PRODUTO Plu	,DESCPROD Descricao	,VLUNIDTRIBUTAVEL [Cst Nf]	,VLRVENDA [Vlr Venda], NUMNOTA NF	
			FROM #TMP_RELACIONANDO A LEFT JOIN [BI].[DBO].[BI_CAD_LOJA2] B ON B.COD_LOJA = A.COD_LOJA WHERE 1=1 
			AND VALIDACAO = 1 
			AND A.COD_LOJA in (`1`)

			ORDER BY VLUNIDTRIBUTAVEL/VLRVENDA DESC
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r
];
r2\[LeftArrow]getTOP23[$lojas]


grid8[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
	 #<0,Red
	,True,Red
	]&;

    stl[val_]:=Style[maF["N,00"][val],Bold,color2@val];
	stl[Null]="0"; 

	grid=maReportGrid[r,"ColumnFormat"->{{"Vlr Venda"}-> stl,{"Cst Nf"}-> "N,00"},"TotalRow"->False];
	title=Style[Row@{"Top Itens"},maTitleFormatOptions]; 
	(maReportFrame[title,grid])
]
grid8[r2];


getTOTAL[codLoja_]:=Module[{sql,s,conn=marcheConn2[],tab},
	sql=" 

			SET NOCOUNT ON;	
			---------------------------------------------------		
			--BUSCANDO NFS DE VENDA--
			---------------------------------------------------	

			SELECT 
					CODFIL COD_LOJA
					,NUMNOTA
					,SERIE
					,CONVERT(DATE,DTNOTA) DTNOTA
					,SUBSTRING (REPLICATE ( '0' ,(10 - LEN(CODIGO_ITEM)) ) + CAST(CODIGO_ITEM AS VARCHAR(10)) ,1,9)*1 AS COD_PRODUTO 
					,DESCPROD
					,CAST(QTDETRIB AS DOUBLE PRECISION) QTDETRIB
					,CAST(VLUNIDTRIBUTAVEL AS DOUBLE PRECISION) VLUNIDTRIBUTAVEL
					,CAST(VLPRODUTO AS DOUBLE PRECISION) VLPRODUTO_TOTAL
			INTO  #TMP_ULTIMAS_NF
			FROM [192.168.0.6].DBNFE.DBO.ST_VW_ITENS_NFSAIDA WITH (NOLOCK) 
			WHERE 1=1
				AND CONVERT(DATE,DTNOTA) >= CONVERT(DATE,GETDATE()-3)
				AND CFOP IN (5552,5552,5152,5152,5151,5151,5551,6551,6102,5102,5102,5101,5102,5102,6102,5102,5102)

			---------------------------------------------------		
			--BUSCANDO VLR DE VENDA--
			---------------------------------------------------	

			SELECT COD_LOJA	,COD_PRODUTO ,CAST(VLR_VENDA AS DOUBLE PRECISION) VLR_VENDA INTO #TMP_VAL_VENDA FROM [BI].[DBO].[VW_PRECOS_VENDA_ATIVOS] WHERE 1=1 AND COD_PRODUTO IN (SELECT COD_PRODUTO FROM #TMP_ULTIMAS_NF) AND VLR_VENDA <> 0

			---------------------------------------------------		
			--BUSCANDO CUSTO DE VENDA DE OUTRAS LOJAS--
			---------------------------------------------------	

			DECLARE @RETORNO AS TABLE 
			( 
				COD_PRODUTO INT
				,VLR_VENDA DOUBLE PRECISION 	
			)

			DECLARE DB_CURSOR CURSOR FOR  
			(SELECT DISTINCT COD_PRODUTO FROM #TMP_ULTIMAS_NF) 

			DECLARE @COD_PRODUTO INT

			OPEN DB_CURSOR   
			FETCH NEXT FROM DB_CURSOR INTO @COD_PRODUTO

			WHILE @@FETCH_STATUS = 0   
			BEGIN   

			INSERT INTO @RETORNO (COD_PRODUTO,VLR_VENDA)
			
						SELECT TOP 1 COD_PRODUTO, VLR_VENDA FROM  #TMP_VAL_VENDA 
						WHERE 1=1
						AND VLR_VENDA <> 0
						AND COD_PRODUTO = @COD_PRODUTO

				   FETCH NEXT FROM DB_CURSOR INTO @COD_PRODUTO   
			END   

			CLOSE DB_CURSOR   
			DEALLOCATE DB_CURSOR

			---------------------------------------------------		
			--RELACIONANDO RESULTADO--
			---------------------------------------------------	
	
			SELECT 
					NF.COD_LOJA
					,NUMNOTA
					,SERIE
					,DTNOTA
					,NF.COD_PRODUTO
					,DESCPROD
					,QTDETRIB
					,VLPRODUTO_TOTAL
					,VLUNIDTRIBUTAVEL
					,ISNULL(VLRVENDA.VLR_VENDA,VLRVENDAGERAL.VLR_VENDA) VLRVENDA
					,CASE WHEN ISNULL(VLRVENDA.VLR_VENDA,VLRVENDAGERAL.VLR_VENDA) < VLUNIDTRIBUTAVEL THEN 1 ELSE 0 END VALIDACAO
			INTO #TMP_RELACIONANDO
			FROM #TMP_ULTIMAS_NF NF 
			LEFT JOIN #TMP_VAL_VENDA VLRVENDA 
				 ON VLRVENDA.COD_LOJA = NF.COD_LOJA AND   VLRVENDA.COD_PRODUTO = NF.COD_PRODUTO
			LEFT JOIN @RETORNO VLRVENDAGERAL 
				 ON  VLRVENDAGERAL.COD_PRODUTO = NF.COD_PRODUTO

			---------------------------------------------------		
			--CRIANDO RESUMO--
			---------------------------------------------------	


			SELECT 
					A.COD_LOJA
					,CASE A.COD_LOJA WHEN 4 THEN 'Cotoxo' WHEN 10 THEN 'SCS' WHEN 19 THEN 'Sumare' WHEN 28 THEN 'Orbis' ELSE [NO_LOJA] END AS Loja
					,ISNULL(SUM(B.VALIDACAO),0) QtdeNfe 
					INTO #TOTAL_TMP
			FROM 
					[BI].[DBO].[BI_CAD_LOJA2] AS A LEFT JOIN #TMP_RELACIONANDO AS B ON B.COD_LOJA = A.COD_LOJA
			WHERE 1=1 
					AND A.COD_LOJA  IN (`1`)
			GROUP BY a.COD_LOJA,NO_LOJA

			UNION
			SELECT 9999 as COD_LOJA, 'Total' as 'Total',  ISNULL(SUM(VALIDACAO),0) AS 'Total' from #TMP_RELACIONANDO
			WHERE 1=1 
					AND COD_LOJA  IN (`1`)
			ORDER BY A.COD_LOJA


			SELECT Loja, QtdeNfe FROM #TOTAL_TMP
			WHERE 1=1 
					AND COD_LOJA IN (`1`)	   

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

	grid=maReportGrid[s,"ColumnFormat"->{{"QtdeNfe"}-> stl},"TotalRow"->True];
	title=Style[Row@{"NF's com Diferen\[CCedilla]a"},maTitleFormatOptions];
	(maReportFrame[title,grid])
]
gridTOTAL[s];


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};

createReport[data_Symbol]:=Module[{s},
	s\[LeftArrow]data;
	Grid[{{gridTOTAL[s],grid8[r2]}},Spacings->{1.5,4},Frame->Transparent]
]
createReport[s];
rep=marcheTemplate[grid=Row@{"Auditoria NF Venda ",$now},createReport[s],1200, $logo];


$fileName1="Auditoria NF Venda em "<>$now2<>".png";
Export["Auditoria NF Venda em "<>$now2<>".png",rep];


$fileName="Rela\[CCedilla]\[ATilde]o de NF's "<>$now2<>".xlsx";
Export["Rela\[CCedilla]\[ATilde]o de NF's "<>$now2<>".xlsx",r["DataAll"]];		


marcheMail[
  		"[GE] Auditoria NF Venda em  " <>$now 
  		,"Auditoria NF Venda em  " <>$now 
		  ,$mailsGerencia
		  ,{$fileName1,$fileName}
]
