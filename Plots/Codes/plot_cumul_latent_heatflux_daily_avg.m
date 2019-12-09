ccc

dataPath = 'C:\Users\anthi182\Desktop\Micromet_data\Merged_csv';

foret_est = readtable(fullfile(dataPath,'Foret_est_merged_data.csv'));
foret_ouest = readtable(fullfile(dataPath,'Foret_ouest_gapfilled_data.csv'));
berge = readtable(fullfile(dataPath,'Berge_gapfilled_data.csv'));
reservoir = readtable(fullfile(dataPath,'Reservoir_gapfilled_data.csv'));
dateRef = datetime(foret_est.date_index);

% Despiking
foret_est.LE(foret_est.LE > 250 | foret_est.LE<-50) = NaN;
foret_ouest.LE(foret_ouest.LE > 250 | foret_ouest.LE<-50) = NaN;
berge.LE(berge.LE > 250 | berge.LE<-50) = NaN;
reservoir.LE(reservoir.LE > 250 | reservoir.LE<-50) = NaN;

% Conversion coefficient
convCoeff = 86.4/2264;

% Select period of interest, reshape (daily sum), and convert to mm/day
id = find(dateRef=={'2019-06-01 00:30:00'}):find(dateRef=={'2019-10-10 00:00:00'});
dateRefid = min(reshape(dateRef(id),48,[]));
foret_est_LE = cumsum(nanmean(reshape(foret_est.LE(id),48,[]),1)*convCoeff);
foret_ouest_LE = cumsum(nanmean(reshape(foret_ouest.LE_gap_filled(id),48,[]),1)*convCoeff);
berge_LE = cumsum(nanmean(reshape(berge.LE_gap_filled(id),48,[]),1)*convCoeff);
reservoir_LE = cumsum(nanmean(reshape(reservoir.LE_gap_filled(id),48,[]),1)*convCoeff);

% LE
figure('Position',[-1920, -175, 1920, 964],'Color','w')
subplot(3,1,1)
hp1 = plot(dateRefid, foret_est_LE); hn
hp2 = plot(dateRefid, foret_ouest_LE);
LEsmoothLand = smooth(nanmean(cat(1,foret_est_LE,foret_ouest_LE),1),10);
idLEsmoothLand = any(~isnan([foret_est_LE; foret_ouest_LE]),1);
hp3 = plot(dateRefid(idLEsmoothLand), LEsmoothLand(idLEsmoothLand));
set(hp1, 'Color',[51 255 51]./255, 'LineStyle','-.')
set(hp2, 'Color',[51 255 51]./255, 'LineStyle','--')
set(hp3, 'Color',[0  153 0]./255, 'LineWidth',2)
xlim([min(dateRefid), max(dateRefid)])
ylim([0 250])
hp4 = plot(dateRefid, zeros(1,numel(dateRefid)));
set(hp4, 'Color','k','LineStyle','--')
ylabel('E cumulé [mm]')
grid on
title('Foret')

subplot(3,1,2)
hp1 = plot(dateRefid, berge_LE); hn
hp2 = plot(dateRefid, reservoir_LE);
LEsmoothWater = smooth(nanmean(cat(1,berge_LE,reservoir_LE),1),10);
idLEsmoothWater = any(~isnan([berge_LE; reservoir_LE]),1);
hp3 = plot(dateRefid(idLEsmoothWater), LEsmoothWater(idLEsmoothWater));
set(hp1, 'Color',[102 178 255]./255, 'LineStyle','-.')
set(hp2, 'Color',[102 178 255]./255, 'LineStyle','--')
set(hp3, 'Color',[0   0   205]./255, 'LineWidth',2)
xlim([min(dateRefid), max(dateRefid)])
ylim([0 250])
hp4 = plot(dateRefid, zeros(1,numel(dateRefid)));
set(hp4, 'Color','k','LineStyle','--')
ylabel('E cumulé [mm]')
grid on
title('Reservoir')

subplot(3,1,3)
diffLE = (nanmean(cat(1,berge_LE,reservoir_LE),1) - nanmean(cat(1,foret_est_LE,foret_ouest_LE),1));
diffLEsmooth = smooth(diffLE,10);
iddiffLEsmooth = ~isnan(diffLE);
hp1 = plot(dateRefid, diffLE); hn
hp2 = plot(dateRefid(iddiffLEsmooth), diffLEsmooth(iddiffLEsmooth));
set(hp1, 'Color',[150 150 150]./255, 'LineStyle','-.')
set(hp2, 'Color',[0   0   0]./255, 'LineWidth',2)
xlim([min(dateRefid), max(dateRefid)])
ylim([-80 80])
hp4 = plot(dateRefid, zeros(1,numel(dateRefid)));
set(hp4, 'Color','k','LineStyle','--')
ylabel('E cumulé [mm]')
grid on
title('Différence')

export_fig('../Plots/cumul_LE_daily_avg.png')