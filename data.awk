BEGIN{
    OFS=","
    srand()
    for(I=0;I<100;I++){
        X=-180+360*rand()
        Y=-90+180*rand()
        W=1+19*rand()
        H=1+19*rand()
        D=X+W-180
        if(D>0) X=X-D
        D=Y+H-90
        if(D>0) Y=Y-D
        print I,X,Y,W,H
    }
}
