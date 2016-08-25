(* ::Package:: *)

(* ::Section:: *)
(*DNV*)


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


$lojas={1,2,3,6,7,9,12,13,17,18,20,21,22,23,24,25,27,29,30,31};
$deParaLoja=maGetDeParaLojas[];
$now=DateString[Now,{"Day","/","Month","/","Year"}];
$now2=DateString[Now,{"Day","-","Month","-","Year"}];
SetDirectory@mrtFileDirectory[];
$mailsGerencia=marcheGetMailFromXML[];


getDNV[codLoja_]:=Module[{sql,r,conn=marcheConn2[],tab},
	sql=" 
				SET NOCOUNT ON;
				SELECT 
							 COD_LOJA
							,NO_LOJA
							,NO_REGIONAL
							INTO #@LOJAS
				FROM [BI].[DBO].[BI_CAD_LOJA2] with (NOLOCK) 
			
				SELECT TOP 10
							NO_LOJA AS LOJA
							,COD_PRODUTO AS PLU
							,DESCRICAO
							,CLASSIF_PRODUTO AS ABC
							,NO_DEPARTAMENTO AS DEPARTAMENTO
							,NO_SECAO AS SE\[CapitalCCedilla]\[CapitalATilde]O
							,QTD_ESTOQUE as ESTOQUE
				FROM
					(
						SELECT
							VP.COD_LOJA
							,NO_LOJA
							,VP.COD_PRODUTO
							,CP.DESCRICAO
							,CLASSIF_PRODUTO
							,CP.NO_DEPARTAMENTO
							,CP.COD_DEPARTAMENTO
							,CP.NO_SECAO
							,CAST (EP.QTD_ESTOQUE AS DOUBLE PRECISION) AS QTD_ESTOQUE
						    ,MAX(VP.DATA) AS LAST_DATE
				FROM
							BI.DBO.BI_VENDA_PRODUTO AS VP WITH (NOLOCK) 
							LEFT JOIN DW.DBO.ESTOQUE AS EP WITH (NOLOCK) ON (VP.COD_PRODUTO = EP.COD_PRODUTO AND VP.COD_LOJA = EP.COD_LOJA)
							LEFT JOIN BI.DBO.BI_CAD_PRODUTO AS CP WITH (NOLOCK) ON (VP.COD_PRODUTO	= CP.COD_PRODUTO)
							LEFT JOIN #@LOJAS as LJ with (NOLOCK)  ON (LJ.COD_LOJA = VP.COD_LOJA)
							WHERE 1 = 1
							AND CONVERT(DATE,VP.DATA) BETWEEN CONVERT(DATE,GETDATE()-5) AND CONVERT(DATE,GETDATE())
							AND CONVERT(DATE,EP.DATA) = CONVERT(DATE,GETDATE())
							AND CP.COD_DEPARTAMENTO = 2
				
				GROUP BY
							VP.COD_LOJA
							,NO_LOJA
							,VP.COD_PRODUTO
							,CP.DESCRICAO
							,CLASSIF_PRODUTO
							,CP.NO_DEPARTAMENTO
							,CP.COD_DEPARTAMENTO
							,CP.NO_SECAO
							,EP.QTD_ESTOQUE
						    --,VAL_CUSTO_REP
					) AS MAX_DATE
				WHERE 1 = 1
							AND DATEDIFF(D,LAST_DATE, GETDATE()) >= 5
							AND COD_DEPARTAMENTO = 2
							AND COD_LOJA IN (`1`)
				ORDER BY
							QTD_ESTOQUE DESC
							
							DROP TABLE #@LOJAS
								
";
	r\[LeftArrow]mrtSQLDataObject[conn,sql,{SQLArgument@@codLoja}];
	r["DateInterval"]={dtIni1,dtFim1};
	r["CodLoja"]=codLoja;
	r["NoLoja"]=codLoja/.$deParaLoja;
	r["FileName"]=ToString@Row@{"DNV ADEGA ",r["NoLoja"],".png"};
	r
]
r[99]=getDNV[$lojas];


i=0;Dynamic@i
Scan[(i++;r[#]=getDNV[#])&,$lojas]


gridDNV[data_Symbol]:=Module[{grid,title,r,color2,stl},
	r\[LeftArrow]data;
	color2=Which[ 
			#<0.04,Darker@Green
			,#<0.06,Orange,
			,True,Red
	]&;

	stl[val_]:=Style[maF["%"][val],Bold,color2@val];
	stl[Null]="";

grid=maReportGrid[r,"ColumnFormat"->{{"ESTOQUE"}-> "N"},"TotalRow"->False];
	title=Style[Row@{"DNV ADEGA ", \!\(\*
StyleBox[
TemplateBox[{"$now"},
"RowDefault"],
StripOnInput->False,
LineColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`],
FrontFaceColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`],
BackFaceColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`],
GraphicsColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`],
FontFamily->"Helvetica",
FontWeight->Bold,
FontColor->RGBColor[0.33333333333333337`, 0.33333333333333337`, 0.33333333333333337`]]\)},maTitleFormatOptions];
	(maReportFrame[title,grid])
]
gridDNV[r[99]];


SetDirectory@FileNameJoin@{mrtFileDirectory[],"reports"};


$fileName=ToString@Row@{"Resumo Top 10 DNV.png"};
Export["Resumo Top 10 DNV.png",gridDNV[r[99]]];


createFile[codLoja_Integer]:=Module[{},
	Export[r[codLoja]["FileName"],gridDNV[r[codLoja]]]
]


i=0;Dynamic@i
Scan[(i++;createFile[#])&,$lojas[[]]]


marcheMail[
  		"[GE] Top 10 DNV Adega Por Loja " <>$now
  		,"DNV Adega em " <>$now
  		,$mailsGerencia
  		,{$fileName,r[1]["FileName"],r[2]["FileName"],r[3]["FileName"],r[6]["FileName"],r[7]["FileName"],r[9]["FileName"],r[12]["FileName"],r[13]["FileName"],r[17]["FileName"],r[18]["FileName"],r[20]["FileName"],r[21]["FileName"],r[22]["FileName"],r[23]["FileName"],r[24]["FileName"],r[25]["FileName"],r[27]["FileName"],r[29]["FileName"],r[30]["FileName"],r[31]["FileName"]}
  ]
