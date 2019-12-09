ccc

dataPath = 'C:\Users\anthi182\Desktop\Micromet_data\Eddypro_data';

berge = readtable(fullfile(dataPath,'Berge','eddypro_Ro2_Berge_temp.csv'));
dateRef = berge.date;

% Conversion and despiking
co2_flux = berge.co2_flux / 1000 * 3600 * 24; % mmol / m2 / day
co2_flux(co2_flux > 1000 | co2_flux < -1000 ) = NaN;

ch4_flux = berge.ch4_flux / 1000 * 3600 * 24; % mmol / m2 / day
ch4_flux(ch4_flux > 4 | ch4_flux < -2) = NaN;


% Select period of interest, reshape (daily sum), and convert to mm/day
id = find(dateRef=='2018-07-01'):find(dateRef=='2019-10-10'); id(end)=[];
dateRefid = min(reshape(dateRef(id),48,[]));
ch4_dailyAvg = nanmean(reshape(ch4_flux(id),48,[]),1);

% LE
figure('Position',[-1920, -175, 1300, 600],'Color','w')
subplot(2,1,1)
hp1 = plot(dateRefid, ch4_dailyAvg); hn
CH4smooth = smooth(ch4_dailyAvg,10);
idCH4smooth = isnan(ch4_dailyAvg);
CH4smooth(idCH4smooth)=NaN;
hp2 = plot(dateRefid, CH4smooth);
set(hp1, 'Color',[150 150 150]./255, 'LineStyle','-.')
set(hp2, 'Color',[0  0  0]./255, 'LineWidth',2)
xlim([min(dateRefid), max(dateRefid)])
ylim([-0.5 3])
hp4 = plot(dateRefid, zeros(1,numel(dateRefid)));
set(hp4, 'Color','k','LineStyle','--')
ylabel('Flux CH4 [mm mol m^{-2} d^{-1}]')
grid on
title('Flux de méthane')

subplot(2,1,2)
ch4_cumul = cumsum(ch4_dailyAvg(~idCH4smooth));
hp1 = plot(dateRefid(~idCH4smooth), ch4_cumul); hn
CH4smooth = smooth(ch4_cumul,10);
hp2 = plot(dateRefid(~idCH4smooth), CH4smooth);
set(hp1, 'Color',[150 150 150]./255, 'LineStyle','-.')
set(hp2, 'Color',[0  0  0]./255, 'LineWidth',2)
xlim([min(dateRefid), max(dateRefid)])
hp4 = plot(dateRefid, zeros(1,numel(dateRefid)));
set(hp4, 'Color','k','LineStyle','--')
ylabel('Flux CH4 [mm mol m^{-2}]')
grid on
title('Flux cumulé de méthane')

export_fig('../Plots/CH4_daily_avg.png')