create OR REPLACE function median_sfunc (
    state float[], data float
) returns float[] as
$$
begin
    if state is null then
        return array[data]; -- if the array is null, return a singleton
    else
        return state || data; -- otherwise append to the existing array
    end if;
end;
$$ language plpgsql;

create OR REPLACE function median_ffunc (
    state float[]
) returns double precision as
$$
begin
    return (state[(array_length(state, 1) + 1)/ 2] 
        + state[(array_length(state, 1) + 2) / 2]) / 2.;
end;
$$ language plpgsql;

create OR REPLACE aggregate median (float) (
    sfunc     = median_sfunc,
    stype     = float[],
    finalfunc = median_ffunc
    );

SELECT median(close) FROM landing.price_history WHERE symbol = 'T';

