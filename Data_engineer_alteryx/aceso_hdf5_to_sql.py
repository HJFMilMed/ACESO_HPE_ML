#################################
#################################
#################################
from ayx import Package
Package.installPackages(['pandas','tables','numpy','SQLAlchemy'])
from ayx import Alteryx
import pandas as pd



#################################
meta_directory_df=Alteryx.read("#1")
meta_directory_df = meta_directory_df.reset_index()


#################################
def extract_patient_id_from_FileName(fileName:str):
    id=fileName.split('_')[-1].replace(".hdf5","")
    return id


#################################
def pivot_dataframe(df,metric_name:str):
    print(type(df))
    columns=list(df.columns.values)
    print(columns)
    columns=columns.remove('idx')
    confidence_name=metric_name+'_confidence'
#     df=df.rename(columns = {'confidence': confidence_name},  inplace = True)
    df_unpivot = pd.melt(df, id_vars='idx', value_vars=columns)
    df_unpivot.loc[df_unpivot["variable"] == "confidence", "variable"] = metric_name+"_confidence"
    print("unpivoted")
    df_unpivot
    return df_unpivot


#################################
outputDF = dict()


#################################
for index, row in meta_directory_df.iterrows():
    full_path=row['FullPath']
    file_name=row['FileName']
    file_df=pd.read_hdf(full_path)
    file_df.to_csv (Alteryx.getWorkflowConstant("Engine.WorkflowDirectory")+"/"+file_name+".csv",index_label="idx", encoding = "utf-8")
    temp_df=pd.read_csv(Alteryx.getWorkflowConstant("Engine.WorkflowDirectory")+"/"+file_name+".csv")
    table_name=row['Directory'].split('\\')[-2]
    metric_name=table_name.replace("agg-piq-torso-1min-","").replace("-per-1min","")
    print(table_name)
    print(metric_name)
    temp_df
    op_df=pivot_dataframe(temp_df,metric_name)
    op_df['table_name']=table_name
    patient_id=extract_patient_id_from_FileName(file_name)
    op_df['patient_id']=patient_id
    op_df
    outputDF[str(index)]=op_df


#################################
finalDF=pd.concat(outputDF)
pi=pd.pivot_table(finalDF, values = 'value', index=['idx','patient_id'], columns = 'variable').reset_index()
Alteryx.write(pi,1)


#################################



#################################
