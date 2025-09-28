import time

def save_data(rslt_dic, table_class):

    aa_list = []
    for item in rslt_dic:
        aa_list.append(
            table_class(
                **item,
            )
        )
        
    strd_dt = time.strftime(('%Y%m%d'))
    table_class.objects.filter(strd_dt=strd_dt).delete()
    
    table_class.objects.bulk_create(aa_list)        
    
    print(f'     [{table_class}] Table Insert Finished')