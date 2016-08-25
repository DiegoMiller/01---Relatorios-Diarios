(* ::Package:: *)

(* ::Section:: *)
(*Margem Real x Margem Referencia*)


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
$dpto={1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,19,21};
$deParaLoja=maGetDeParaLojas[];
$deParaDpto=maGetDeParaDpto[];
$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
$mails=marcheGetMails[];
$cargosLoja={"GERENTE","COORD_PERECIVEL","COORD_RELACIONAMENTO","COORD_NAO_PERECIVEL","REGIONAL","COORD_PREVENCAO","FISCAL_PREVENCAO"};
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];
SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};


getMargem[Dpto_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 
		
		SET NOCOUNT ON;
		-------------------------------------------------------------------------------------------------------------------------------------
		--CADASTRO DE PRODUTOS PRECO E MARGEM DE REFERENCIA
		-------------------------------------------------------------------------------------------------------------------------------------
		SELECT 	
					COD_LOJA
					,CAD.COD_PRODUTO
					,CAD.COD_DEPARTAMENTO
					,CAD.COD_SECAO
					,CAD.COD_GRUPO
					,COD_SUB_GRUPO
					,NO_DEPARTAMENTO
					,NO_SECAO
					,NO_GRUPO
					,NO_SUBGRUPO
					,CAD.DESCRICAO
					,CAD.FORA_LINHA
					,PESADO
					,UNIDADE_COMPRA
					,UNIDADE_VENDA
					,CAD.COD_FORNECEDOR
					--,CF.DESCRICAO AS DES_FORNECEDOR
					,CLASSIF_PRODUTO
					,CAD.COD_USUARIO
					,NO_COMPRADOR
					,PNP
					,CAST(ISNULL(VLR_MRGREF,0) AS DOUBLE PRECISION) AS VLR_MRGREF
					,CAST(VLR_VENDA AS DOUBLE PRECISION) AS VLR_VENDA
					,CAST(VLR_OFERTA AS DOUBLE PRECISION) AS VLR_OFERTA
					,CAST(VLR_VCMARCHE AS DOUBLE PRECISION) AS VLR_VCMARCHE
		INTO #TEMP_CAD_PROD		
				FROM 
						BI.DBO.BI_LINHA_PRODUTOS AS LINHA WITH (NOLOCK)		
				INNER JOIN
						BI.DBO.[BI_CAD_PRODUTO] AS CAD WITH (NOLOCK)
				ON LINHA.COD_PRODUTO = CAD.COD_PRODUTO
				INNER JOIN 
						[BI].[DBO].[COMPRAS_CAD_COMPRADOR] AS COMPRAS WITH (NOLOCK)
				ON CAD.COD_USUARIO = COMPRAS.COD_USUARIO
				--LEFT JOIN 
				--		[BI].[DBO].[BI_CAD_FORNECEDOR] AS CF
				--ON CAD.COD_FORNECEDOR = CF.COD_FORNECEDOR
		WHERE 1=1 
				AND CAD.COD_DEPARTAMENTO IN (`1`)			
		
		-------------------------------------------------------------------------------------------------------------------------------------
		--CADASTRO DE IMPOSTOS NO AX
		-------------------------------------------------------------------------------------------------------------------------------------
		SELECT 
					COD_PRODUTO,
					ICMS_ALIQUOTA AS ICMS_ALIQUOTA_ATUAL,
					ICMS_REDUCAO AS ICMS_REDUCAO_ATUAL,
					COFINS_ALIQUOTA AS ASCOFINS_ALIQUOTA_ATUAL,
					PIS_ALIQUOTA AS ASPIS_ALIQUOTA_ATUAL
		INTO #TEMP_IMPOSTOS_AX
		FROM 
				AX2009_INTEGRACAO.DBO.TAB_PRODUTO_IMPOSTOS_VENDA WITH (NOLOCK)
		-------------------------------------------------------------------------------------------------------------------------------------		
		--ULTIMO CUSTO MEDIO CALCULADO
		-------------------------------------------------------------------------------------------------------------------------------------
		
		--SELECT 
		--			COD_LOJA
		--			,COD_PRODUTO
		--			,CAST(VLR_CUSTO_MEDIO AS DOUBLE PRECISION) AS VLRCMV
		--			,CAST(VLR_CUSTO_AQUISICAO AS DOUBLE PRECISION) AS VLR_CUSTO_AQUISICAO
		--INTO #TEMP_CUSTO 
		--FROM 
		--		DW.DBO.CUSTO_MEDIO WITH (NOLOCK) 
		--WHERE 1=1 
		--		AND IDCALC = 1
				
				
		SELECT DISTINCT
					COD_LOJA
					,COD_PRODUTO
					,CAST(VLR_CUSTO_MEDIO AS DOUBLE PRECISION) AS VLRCMV
		INTO #TEMP_CUSTO 			
		FROM 
				DW.DBO.CMV WITH (NOLOCK) 
		WHERE 1=1 
				AND IDCALC = 1
				
		-------------------------------------------------------------------------------------------------------------------------------------
		--VENDAS DISTINTAS DOS ULTIMOS 10 DIAS
		-------------------------------------------------------------------------------------------------------------------------------------
			--SELECT DISTINCT 
		--			COD_LOJA
		--			,COD_PRODUTO 
		--INTO #TEMP_VENDA 
		--FROM 
		--		DW.DBO.BI_ANAL_TICKET WITH (NOLOCK)
		--WHERE 1=1
		--		AND DATA >= CONVERT(DATE,GETDATE()-30)
				
		SELECT DISTINCT 
					COD_LOJA
					,COD_PRODUTO 
		INTO #TEMP_VENDA 
		FROM 
				bi.dbo.BI_VENDA_PRODUTO WITH (NOLOCK)
		WHERE 1=1
				AND DATA >= CONVERT(DATE,GETDATE()-10)		
				
		-------------------------------------------------------------------------------------------------------------------------------------
		--JUNTANDO DADOS E FAZENDO A CONTA
		-------------------------------------------------------------------------------------------------------------------------------------
		SELECT 
					CAD.COD_LOJA
					,CAD.COD_PRODUTO
					,DESCRICAO
					,COD_DEPARTAMENTO
					,NO_DEPARTAMENTO
					,NO_COMPRADOR
					,VLRCMV
					,VLR_VENDA
					,VLR_OFERTA
					,VLR_VCMARCHE
					,ICMS_ALIQUOTA_ATUAL
					,ICMS_REDUCAO_ATUAL
					,ASCOFINS_ALIQUOTA_ATUAL
					,ASPIS_ALIQUOTA_ATUAL
					,(VLR_MRGREF/100) AS VLR_MRGREF
					,(VLR_VENDA-VLRCMV-((VLR_VENDA*((ICMS_ALIQUOTA_ATUAL/100)*(1-(ICMS_REDUCAO_ATUAL/100))))+(VLR_VENDA*(ASCOFINS_ALIQUOTA_ATUAL/100))+(VLR_VENDA*(ASPIS_ALIQUOTA_ATUAL/100)))) AS LB_CONSUMIDOR
					,(VLR_OFERTA-VLRCMV-((VLR_OFERTA*((ICMS_ALIQUOTA_ATUAL/100)*(1-(ICMS_REDUCAO_ATUAL/100))))+(VLR_OFERTA*(ASCOFINS_ALIQUOTA_ATUAL/100))+(VLR_OFERTA*(ASPIS_ALIQUOTA_ATUAL/100)))) AS LB_OFERTA
					,(VLR_VCMARCHE-VLRCMV-((VLR_VCMARCHE*((ICMS_ALIQUOTA_ATUAL/100)*(1-(ICMS_REDUCAO_ATUAL/100))))+(VLR_VCMARCHE*(ASCOFINS_ALIQUOTA_ATUAL/100))+(VLR_VCMARCHE*(ASPIS_ALIQUOTA_ATUAL/100)))) AS LB_VCMARCHE
					,(VLR_VENDA*((ICMS_ALIQUOTA_ATUAL/100)*(1-(ICMS_REDUCAO_ATUAL/100))))+(VLR_VENDA*(ASCOFINS_ALIQUOTA_ATUAL/100))+(VLR_VENDA*(ASPIS_ALIQUOTA_ATUAL/100)) AS Vlr_Impostos
					,(VLR_VENDA*((ICMS_ALIQUOTA_ATUAL/100)*(1-(ICMS_REDUCAO_ATUAL/100)))) AS ICMS
					,(VLR_VENDA*(ASCOFINS_ALIQUOTA_ATUAL/100))+(VLR_VENDA*(ASPIS_ALIQUOTA_ATUAL/100)) AS PIS_COFINS
					,(ICMS_ALIQUOTA_ATUAL/100)*(1-(ICMS_REDUCAO_ATUAL/100)) AS ALIQUOTA_AX
		INTO #JUNTANDO_DADOS
		FROM	
					#TEMP_CAD_PROD AS CAD
				LEFT JOIN
					#TEMP_CUSTO AS CUSTO 
				ON CUSTO.COD_LOJA = CAD.COD_LOJA AND CUSTO.COD_PRODUTO = CAD.COD_PRODUTO
				LEFT JOIN 
					#TEMP_IMPOSTOS_AX AS A 
				ON CAD.COD_PRODUTO = A.COD_PRODUTO
		WHERE 1=1 
		AND COD_DEPARTAMENTO IN (`1`)	
			
		-------------------------------------------------------------------------------------------------------------------------------------
		--SELECIONANDO O TOP 10 ATRAVES DA MARGEM REAL X REFERENCIA DOS PRODUTOS COM VENDA NOS ULTIMOS 10 DIAS
		-------------------------------------------------------------------------------------------------------------------------------------
			
		SELECT  TOP 10
					 NO_LOJA AS Loja
					,A.COD_PRODUTO AS PLU
					,DESCRICAO AS Descri\[CCedilla]\[ATilde]o
					,NO_DEPARTAMENTO AS Dpto
					,NO_COMPRADOR As Comprador
					,VLRCMV CMV
				    ,VLR_VENDA [Vlr Consumidor]
					,VLR_OFERTA [Vlr Oferta]
					,VLR_VCMARCHE [Vlr Vc Marche]
					,ICMS
					,PIS_COFINS AS [PIS COFINS]
					,CAST((LB_CONSUMIDOR)/VLR_VENDA AS DOUBLE PRECISION) [Marg Consumidor]
					,CAST((LB_OFERTA)/VLR_VENDA AS DOUBLE PRECISION) [Marg Oferta]
					,CAST((LB_VCMARCHE)/VLR_VENDA AS DOUBLE PRECISION) [Marg Vc Marche]
					,VLR_MRGREF AS [Marg Ref]
					,CAST((LB_CONSUMIDOR)/VLR_VENDA AS DOUBLE PRECISION)-CAST(VLR_MRGREF AS DOUBLE PRECISION) AS [Marg Real X Marg Ref]
					,((VLR_VENDA)-(PIS_COFINS+ICMS)-(VLRCMV)) - (VLR_MRGREF*VLR_VENDA) as Saldo
					--,+VLRCMV/(1-VLR_MRGREF-((ICMS+PIS_COFINS)/VLR_VENDA)) Saldo (REF O VALOR DE VENDA)
		FROM 
				#JUNTANDO_DADOS AS A 
				LEFT JOIN 
						[BI].[DBO].[BI_CAD_LOJA2] AS LOJA 
				ON LOJA.COD_LOJA = A.COD_LOJA
				INNER JOIN 
						#TEMP_VENDA AS VENDA 
				ON VENDA.COD_LOJA = A.COD_LOJA AND VENDA.COD_PRODUTO = A.COD_PRODUTO
				
		WHERE 1=1 
				AND CAST((LB_CONSUMIDOR)/VLR_VENDA AS DOUBLE PRECISION)-CAST(VLR_MRGREF AS DOUBLE PRECISION) <0
				AND VLR_VENDA > 0
		ORDER BY 
				((VLR_VENDA)-(PIS_COFINS+ICMS)-(VLRCMV)) - (VLR_MRGREF*VLR_VENDA) ASC
				,CAST((LB_CONSUMIDOR)/VLR_VENDA AS DOUBLE PRECISION)-CAST(VLR_MRGREF AS DOUBLE PRECISION) ASC
				
	--DROP TABLE #JUNTANDO_DADOS
	--DROP TABLE #TEMP_CAD_PROD
	--DROP TABLE #TEMP_IMPOSTOS_AX
	--DROP TABLE #TEMP_CUSTO
	--DROP TABLE #TEMP_VENDA
		
";
r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@Dpto}];
	r["Dpto"]=Dpto;
	r["NO_DEPARTAMENTO"]=Dpto/.$deParaDpto;
	r["FileName"]=ToString@Row@{"Margem Real x Margem Ref ",r["NO_DEPARTAMENTO"],".png"};
	r
]
r[80]=getMargem[$dpto];


i=0;Dynamic@i
Scan[(i++;r[#]=getMargem[#])&,$dpto]


gridMARGEM[data_Symbol]:=Module[{grid,title,r,color2,stl,Dpto},
	
r\[LeftArrow]data;
			color2=Which[
			#<0.04,Red
			,#<0.06,Orange
			,True,Lighter@Blue
	]&;

	stl[val_]:=Style[maF["%"][val],Bold,color2@val];
	stl[Null]="";


	grid=maReportGrid[r,"ColumnFormat"->{{"CMV", "Vlr Consumidor", "Vlr Oferta", "Vlr Vc Marche", "ICMS", "PIS COFINS","Saldo"}-> "N,00",{"Marg Consumidor","Marg Oferta","Marg Vc Marche","Marg Ref","Marg Real X Marg Ref"}-> stl},"TotalRow"->False];
	title=Style[Row@{"Margem Real x Margem Ref ", $now},maTitleFormatOptions];
	(maReportFrame[title,grid])
]
gridMARGEM[r[80]]


$DptoAll=Export[ToString@Row@{"Margem Real x Margem Ref Rede.png"},gridMARGEM[r[80]]];


createFile[$dpto_Integer]:=Module[{},
	Export[r[$dpto]["FileName"],gridMARGEM[r[$dpto]]]
]

i=0;Dynamic@i
Scan[(i++;createFile[#])&,$dpto[[]]]


marcheMail[
  		"[GE] Margem Real x Margem Ref " <>$now
  		,"[GE] Margem Real x Margem Ref"
  		,$mailsGerencia
  		,{$DptoAll,r[1]["FileName"],r[2]["FileName"],r[3]["FileName"],r[4]["FileName"],r[5]["FileName"],r[6]["FileName"],r[7]["FileName"],r[8]["FileName"],r[9]["FileName"],r[10]["FileName"],r[11]["FileName"],r[12]["FileName"],r[13]["FileName"],r[14]["FileName"],r[15]["FileName"],r[19]["FileName"],r[21]["FileName"]}
]


(*getMargemExcel[Dpto_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 
		
		SET NOCOUNT ON;
		-------------------------------------------------------------------------------------------------------------------------------------
		--CADASTRO DE PRODUTOS PRECO E MARGEM DE REFERENCIA
		-------------------------------------------------------------------------------------------------------------------------------------
		SELECT 	
					COD_LOJA
					,CAD.COD_PRODUTO
					,CAD.COD_DEPARTAMENTO
					,CAD.COD_SECAO
					,CAD.COD_GRUPO
					,COD_SUB_GRUPO
					,NO_DEPARTAMENTO
					,NO_SECAO
					,NO_GRUPO
					,NO_SUBGRUPO
					,CAD.DESCRICAO
					,CAD.FORA_LINHA
					,PESADO
					,UNIDADE_COMPRA
					,UNIDADE_VENDA
					,CAD.COD_FORNECEDOR
					--,CF.DESCRICAO AS DES_FORNECEDOR
					,CLASSIF_PRODUTO
					,CAD.COD_USUARIO
					,NO_COMPRADOR
					,PNP
					,CAST(ISNULL(VLR_MRGREF,0) AS DOUBLE PRECISION) AS VLR_MRGREF
					,CAST(VLR_VENDA AS DOUBLE PRECISION) AS VLR_VENDA
					,CAST(VLR_OFERTA AS DOUBLE PRECISION) AS VLR_OFERTA
					,CAST(VLR_VCMARCHE AS DOUBLE PRECISION) AS VLR_VCMARCHE
		INTO #TEMP_CAD_PROD		
				FROM 
						BI.DBO.BI_LINHA_PRODUTOS AS LINHA WITH (NOLOCK)		
				INNER JOIN
						BI.DBO.[BI_CAD_PRODUTO] AS CAD WITH (NOLOCK)
				ON LINHA.COD_PRODUTO = CAD.COD_PRODUTO
				INNER JOIN 
						[BI].[DBO].[COMPRAS_CAD_COMPRADOR] AS COMPRAS WITH (NOLOCK)
				ON CAD.COD_USUARIO = COMPRAS.COD_USUARIO
				--LEFT JOIN 
				--		[BI].[DBO].[BI_CAD_FORNECEDOR] AS CF
				--ON CAD.COD_FORNECEDOR = CF.COD_FORNECEDOR
		WHERE 1=1 
				AND CAD.COD_DEPARTAMENTO IN (`1`)			
		
		-------------------------------------------------------------------------------------------------------------------------------------
		--CADASTRO DE IMPOSTOS NO AX
		-------------------------------------------------------------------------------------------------------------------------------------
		SELECT 
					COD_PRODUTO,
					ICMS_ALIQUOTA AS ICMS_ALIQUOTA_ATUAL,
					ICMS_REDUCAO AS ICMS_REDUCAO_ATUAL,
					COFINS_ALIQUOTA AS ASCOFINS_ALIQUOTA_ATUAL,
					PIS_ALIQUOTA AS ASPIS_ALIQUOTA_ATUAL
		INTO #TEMP_IMPOSTOS_AX
		FROM 
				AX2009_INTEGRACAO.DBO.TAB_PRODUTO_IMPOSTOS_VENDA WITH (NOLOCK)
		-------------------------------------------------------------------------------------------------------------------------------------		
		--ULTIMO CUSTO MEDIO CALCULADO
		-------------------------------------------------------------------------------------------------------------------------------------
		
		--SELECT 
		--			COD_LOJA
		--			,COD_PRODUTO
		--			,CAST(VLR_CUSTO_MEDIO AS DOUBLE PRECISION) AS VLRCMV
		--			,CAST(VLR_CUSTO_AQUISICAO AS DOUBLE PRECISION) AS VLR_CUSTO_AQUISICAO
		--INTO #TEMP_CUSTO 
		--FROM 
		--		DW.DBO.CUSTO_MEDIO WITH (NOLOCK) 
		--WHERE 1=1 
		--		AND IDCALC = 1
				
				
		SELECT DISTINCT
					COD_LOJA
					,COD_PRODUTO
					,CAST(VLR_CUSTO_MEDIO AS DOUBLE PRECISION) AS VLRCMV
		INTO #TEMP_CUSTO 			
		FROM 
				DW.DBO.CMV WITH (NOLOCK) 
		WHERE 1=1 
				AND IDCALC = 1
				
		-------------------------------------------------------------------------------------------------------------------------------------
		--VENDAS DISTINTAS DOS ULTIMOS 10 DIAS
		-------------------------------------------------------------------------------------------------------------------------------------
			--SELECT DISTINCT 
		--			COD_LOJA
		--			,COD_PRODUTO 
		--INTO #TEMP_VENDA 
		--FROM 
		--		DW.DBO.BI_ANAL_TICKET WITH (NOLOCK)
		--WHERE 1=1
		--		AND DATA >= CONVERT(DATE,GETDATE()-30)
				
		SELECT DISTINCT 
					COD_LOJA
					,COD_PRODUTO 
		INTO #TEMP_VENDA 
		FROM 
				bi.dbo.BI_VENDA_PRODUTO WITH (NOLOCK)
		WHERE 1=1
				AND DATA >= CONVERT(DATE,GETDATE()-10)		
				
		-------------------------------------------------------------------------------------------------------------------------------------
		--JUNTANDO DADOS E FAZENDO A CONTA
		-------------------------------------------------------------------------------------------------------------------------------------
		SELECT 
					CAD.COD_LOJA
					,CAD.COD_PRODUTO
					,DESCRICAO
					,COD_DEPARTAMENTO
					,NO_DEPARTAMENTO
					,NO_COMPRADOR
					,VLRCMV
					,VLR_VENDA
					,VLR_OFERTA
					,VLR_VCMARCHE
					,ICMS_ALIQUOTA_ATUAL
					,ICMS_REDUCAO_ATUAL
					,ASCOFINS_ALIQUOTA_ATUAL
					,ASPIS_ALIQUOTA_ATUAL
					,(VLR_MRGREF/100) AS VLR_MRGREF
					,(VLR_VENDA-VLRCMV-((VLR_VENDA*((ICMS_ALIQUOTA_ATUAL/100)*(1-(ICMS_REDUCAO_ATUAL/100))))+(VLR_VENDA*(ASCOFINS_ALIQUOTA_ATUAL/100))+(VLR_VENDA*(ASPIS_ALIQUOTA_ATUAL/100)))) AS LB_CONSUMIDOR
					,(VLR_OFERTA-VLRCMV-((VLR_OFERTA*((ICMS_ALIQUOTA_ATUAL/100)*(1-(ICMS_REDUCAO_ATUAL/100))))+(VLR_OFERTA*(ASCOFINS_ALIQUOTA_ATUAL/100))+(VLR_OFERTA*(ASPIS_ALIQUOTA_ATUAL/100)))) AS LB_OFERTA
					,(VLR_VCMARCHE-VLRCMV-((VLR_VCMARCHE*((ICMS_ALIQUOTA_ATUAL/100)*(1-(ICMS_REDUCAO_ATUAL/100))))+(VLR_VCMARCHE*(ASCOFINS_ALIQUOTA_ATUAL/100))+(VLR_VCMARCHE*(ASPIS_ALIQUOTA_ATUAL/100)))) AS LB_VCMARCHE
					,(VLR_VENDA*((ICMS_ALIQUOTA_ATUAL/100)*(1-(ICMS_REDUCAO_ATUAL/100))))+(VLR_VENDA*(ASCOFINS_ALIQUOTA_ATUAL/100))+(VLR_VENDA*(ASPIS_ALIQUOTA_ATUAL/100)) AS Vlr_Impostos
					,(VLR_VENDA*((ICMS_ALIQUOTA_ATUAL/100)*(1-(ICMS_REDUCAO_ATUAL/100)))) AS ICMS
					,(VLR_VENDA*(ASCOFINS_ALIQUOTA_ATUAL/100))+(VLR_VENDA*(ASPIS_ALIQUOTA_ATUAL/100)) AS PIS_COFINS
					,(ICMS_ALIQUOTA_ATUAL/100)*(1-(ICMS_REDUCAO_ATUAL/100)) AS ALIQUOTA_AX
		INTO #JUNTANDO_DADOS
		FROM	
					#TEMP_CAD_PROD AS CAD
				LEFT JOIN
					#TEMP_CUSTO AS CUSTO 
				ON CUSTO.COD_LOJA = CAD.COD_LOJA AND CUSTO.COD_PRODUTO = CAD.COD_PRODUTO
				LEFT JOIN 
					#TEMP_IMPOSTOS_AX AS A 
				ON CAD.COD_PRODUTO = A.COD_PRODUTO
		WHERE 1=1 
		AND COD_DEPARTAMENTO IN (`1`)	
			
		-------------------------------------------------------------------------------------------------------------------------------------
		--SELECIONANDO O TOP 10 ATRAVES DA MARGEM REAL X REFERENCIA DOS PRODUTOS COM VENDA NOS ULTIMOS 10 DIAS
		-------------------------------------------------------------------------------------------------------------------------------------
			
		SELECT  TOP 100
					 NO_LOJA AS Loja
					,A.COD_PRODUTO AS PLU
					,DESCRICAO AS Descri\[CCedilla]\[ATilde]o
					,NO_DEPARTAMENTO AS Dpto
					,NO_COMPRADOR As Comprador
					,VLRCMV CMV
				    ,VLR_VENDA [Vlr Consumidor]
					,VLR_OFERTA [Vlr Oferta]
					,VLR_VCMARCHE [Vlr Vc Marche]
					,ICMS
					,PIS_COFINS AS [PIS COFINS]
					,CAST((LB_CONSUMIDOR)/VLR_VENDA AS DOUBLE PRECISION) [Marg Consumidor]
					,CAST((LB_OFERTA)/VLR_VENDA AS DOUBLE PRECISION) [Marg Oferta]
					,CAST((LB_VCMARCHE)/VLR_VENDA AS DOUBLE PRECISION) [Marg Vc Marche]
					,VLR_MRGREF AS [Marg Ref]
					,CAST((LB_CONSUMIDOR)/VLR_VENDA AS DOUBLE PRECISION)-CAST(VLR_MRGREF AS DOUBLE PRECISION) AS [Marg Real X Marg Ref]
					,((VLR_VENDA)-(PIS_COFINS+ICMS)-(VLRCMV)) - (VLR_MRGREF*VLR_VENDA) as Saldo
					--,+VLRCMV/(1-VLR_MRGREF-((ICMS+PIS_COFINS)/VLR_VENDA)) Saldo (REF O VALOR DE VENDA)
		FROM 
				#JUNTANDO_DADOS AS A 
				LEFT JOIN 
						[BI].[DBO].[BI_CAD_LOJA2] AS LOJA 
				ON LOJA.COD_LOJA = A.COD_LOJA
				INNER JOIN 
						#TEMP_VENDA AS VENDA 
				ON VENDA.COD_LOJA = A.COD_LOJA AND VENDA.COD_PRODUTO = A.COD_PRODUTO
				
		WHERE 1=1 
				AND CAST((LB_CONSUMIDOR)/VLR_VENDA AS DOUBLE PRECISION)-CAST(VLR_MRGREF AS DOUBLE PRECISION) <0
				AND VLR_VENDA > 0
		ORDER BY 
				((VLR_VENDA)-(PIS_COFINS+ICMS)-(VLRCMV)) - (VLR_MRGREF*VLR_VENDA) ASC
				,CAST((LB_CONSUMIDOR)/VLR_VENDA AS DOUBLE PRECISION)-CAST(VLR_MRGREF AS DOUBLE PRECISION) ASC
				
	--DROP TABLE #JUNTANDO_DADOS
	--DROP TABLE #TEMP_CAD_PROD
	--DROP TABLE #TEMP_IMPOSTOS_AX
	--DROP TABLE #TEMP_CUSTO
	--DROP TABLE #TEMP_VENDA
		
";
r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@Dpto}];
	r["Dpto"]=Dpto;
	r["NO_DEPARTAMENTO"]=Dpto/.$deParaDpto;
	r["FileName"]=ToString@Row@{"Margem Real x Margem Ref ",r["NO_DEPARTAMENTO"]," ",$now2,".png"};
	r
]
r[90]=getMargemExcel[$dpto];
*)


(*
SetDirectory@FileNameJoin@{mrtFileDirectory[],"reportsExcel"};
$fileNameINV="Top 10 Margem "<>$now2<>".xlsx";
Export["Top 10 Margem "<>$now2<>".xlsx",r[90]["DataAll"]];
*)
